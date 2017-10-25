/*-------------------------------------------------------------------------
 *
 * itup.h
 *	  POSTGRES index tuple definitions.
 *
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/itup.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef ITUP_H
#define ITUP_H

#include "access/tupdesc.h"
#include "access/tupmacs.h"
#include "storage/bufpage.h"
#include "storage/itemptr.h"

/*
 * Index tuple header structure
 *
 * All index tuples start with IndexTupleData.  If the HasNulls bit is set,
 * this is followed by an IndexAttributeBitMapData.  The index attribute
 * values follow, beginning at a MAXALIGN boundary.
 *
 * Note that the space allocated for the bitmap does not vary with the number
 * of attributes; that is because we don't have room to store the number of
 * attributes in the header.  Given the MAXALIGN constraint there's no space
 * savings to be had anyway, for usual values of INDEX_MAX_KEYS.
 */

typedef struct IndexTupleData
{
	ItemPointerData t_tid;		/* reference TID to heap tuple */

	/* ---------------
	 * t_info is laid out in the following fashion:
	 *
	 * 15th (high) bit: has nulls
	 * 14th bit: has var-width attributes
	 * 13th bit: unused
	 * 12-0 bit: size of tuple
	 * ---------------
	 */

	unsigned short t_info;		/* various info about tuple */

} IndexTupleData;				/* MORE DATA FOLLOWS AT END OF STRUCT */

typedef IndexTupleData *IndexTuple;

typedef struct IndexTupleExtData
{
	ItemPointerExtData t_tid;	/* reference TID to heap tuple */
	unsigned short t_info;		/* various info about tuple */

} IndexTupleExtData;

typedef IndexTupleExtData *IndexTupleExt;

typedef struct IndexAttributeBitMapData
{
	bits8		bits[(INDEX_MAX_KEYS + 8 - 1) / 8];
}			IndexAttributeBitMapData;

typedef IndexAttributeBitMapData * IndexAttributeBitMap;

/*
 * t_info manipulation macros
 */
#define INDEX_SIZE_MASK 0x1FFF
/* bit 0x2000 is reserved for index-AM specific usage */
#define INDEX_VAR_MASK	0x4000
#define INDEX_NULL_MASK 0x8000

#define IndexTupleSize(itup)		((Size) (((IndexTuple) (itup))->t_info & INDEX_SIZE_MASK))
#define IndexTupleDSize(itup)		((Size) ((itup).t_info & INDEX_SIZE_MASK))
#define IndexTupleHasNulls(itup)	((((IndexTuple) (itup))->t_info & INDEX_NULL_MASK))
#define IndexTupleHasVarwidths(itup) ((((IndexTuple) (itup))->t_info & INDEX_VAR_MASK))



typedef struct InMemoryIndexTupleData
{
	ItemPointerExtData	t_tid;			/* reference TID to heap tuple */
	unsigned short		t_info;			/* various info about tuple */
	IndexAttributeBitMapData t_nulls;	/* nulls bitmap */
	Size				t_len;			/* tuple data size only */
	void			   *t_data;			/* tuple data */
} InMemoryIndexTupleData;

typedef InMemoryIndexTupleData *InMemoryIndexTuple;



/*
 * Takes an infomask as argument (primarily because this needs to be usable
 * at index_form_tuple time so enough space is allocated).
 */
#define IndexInfoFindDataOffset(t_info) \
( \
	(!((t_info) & INDEX_NULL_MASK)) ? \
	( \
		(Size)MAXALIGN(sizeof(IndexTupleData)) \
	) \
	: \
	( \
		(Size)MAXALIGN(sizeof(IndexTupleData) + sizeof(IndexAttributeBitMapData)) \
	) \
)

/* ----------------
 *		index_getattr
 *
 *		This gets called many times, so we macro the cacheable and NULL
 *		lookups, and call nocache_index_getattr() for the rest.
 *
 * ----------------
 */
#define index_getattr(tup, attnum, tupleDesc, isnull) \
( \
	AssertMacro(PointerIsValid(isnull) && (attnum) > 0), \
	*(isnull) = false, \
	!IndexTupleHasNulls(tup) ? \
	( \
		(tupleDesc)->attrs[(attnum)-1]->attcacheoff >= 0 ? \
		( \
			fetchatt((tupleDesc)->attrs[(attnum)-1], \
			(char *) (tup) + IndexInfoFindDataOffset((tup)->t_info) \
			+ (tupleDesc)->attrs[(attnum)-1]->attcacheoff) \
		) \
		: \
			nocache_index_getattr((tup), (attnum), (tupleDesc)) \
	) \
	: \
	( \
		(att_isnull((attnum)-1, (char *)(tup) + sizeof(IndexTupleData))) ? \
		( \
			*(isnull) = true, \
			(Datum)NULL \
		) \
		: \
		( \
			nocache_index_getattr((tup), (attnum), (tupleDesc)) \
		) \
	) \
)

#define im_index_getattr(tup, attnum, tupleDesc, isnull) \
( \
	AssertMacro(PointerIsValid(isnull) && (attnum) > 0), \
	*(isnull) = false, \
	(!(((InMemoryIndexTuple) (tup))->t_info & INDEX_NULL_MASK)) ? \
	( \
		(tupleDesc)->attrs[(attnum)-1]->attcacheoff >= 0 ? \
		( \
			fetchatt((tupleDesc)->attrs[(attnum)-1], \
			(char *) ((InMemoryIndexTuple) (tup))->t_data \
			+ (tupleDesc)->attrs[(attnum)-1]->attcacheoff) \
		) \
		: \
			im_nocache_index_getattr((tup), (attnum), (tupleDesc)) \
	) \
	: \
	( \
		(att_isnull((attnum)-1, (char *)(tup) + sizeof(IndexTupleData))) ? \
		( \
			*(isnull) = true, \
			(Datum)NULL \
		) \
		: \
		( \
			im_nocache_index_getattr((tup), (attnum), (tupleDesc)) \
		) \
	) \
)

/* TODO: add different size options for global and local index tuple */
#define im_index_tuple_size(tup) \
	MAXALIGN(IndexInfoFindDataOffset((tup)->t_info) + (tup)->t_len)


/*
 * MaxIndexTuplesPerPage is an upper bound on the number of tuples that can
 * fit on one index page.  An index tuple must have either data or a null
 * bitmap, so we can safely assume it's at least 1 byte bigger than a bare
 * IndexTupleData struct.  We arrive at the divisor because each tuple
 * must be maxaligned, and it must have an associated item pointer.
 */
#define MinIndexTupleSize MAXALIGN(sizeof(IndexTupleData) + 1)
#define MaxIndexTuplesPerPage	\
	((int) ((BLCKSZ - SizeOfPageHeaderData) / \
			(MAXALIGN(sizeof(IndexTupleData) + 1) + sizeof(ItemIdData))))

#define FreeInMemoryIndexTuple(imtup) \
	do { \
		pfree((imtup)->t_data); \
		pfree(imtup); \
	} while (0)


/* routines in indextuple.c */
extern IndexTuple index_form_tuple(TupleDesc tupleDescriptor,
				 Datum *values, bool *isnull);
extern void index_form_inmemory_tuple(TupleDesc tupleDescriptor,
						  Datum *values,
						  bool *isnull,
						  InMemoryIndexTuple itup);
void *index_form_tuple_data(TupleDesc tupleDescriptor,
					  Datum *values, bool *isnull,
					  Size *data_size, unsigned short *ret_infomask);
extern Datum nocache_index_getattr(IndexTuple tup, int attnum,
					  TupleDesc tupleDesc);
extern Datum im_nocache_index_getattr(InMemoryIndexTuple tup,
						 int attnum,
						 TupleDesc tupleDesc);
extern void index_deform_tuple(IndexTuple tup, TupleDesc tupleDescriptor,
				   Datum *values, bool *isnull);
extern void im_index_deform_tuple(InMemoryIndexTuple tup,
					  TupleDesc tupleDescriptor,
					  Datum *values, bool *isnull);
extern IndexTuple CopyIndexTuple(IndexTuple source);

extern IndexTuple inmemory_index_tuple_to_physical_format(InMemoryIndexTuple itup);
extern InMemoryIndexTuple physical_index_tuple_to_inmemory_format(IndexTuple tup);

#endif							/* ITUP_H */
