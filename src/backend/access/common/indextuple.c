/*-------------------------------------------------------------------------
 *
 * indextuple.c
 *	   This file contains index tuple accessor and mutator routines,
 *	   as well as various tuple utilities.
 *
 * Portions Copyright (c) 1996-2017, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/common/indextuple.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/itup.h"
#include "access/tuptoaster.h"


static Datum nocache_index_getattr_common(char *tuple_data,
							 char *nulls,
							 unsigned infomask,
							 int attnum,
							 TupleDesc tupleDesc);

/* ----------------------------------------------------------------
 *				  index_ tuple interface routines
 * ----------------------------------------------------------------
 */

/* ----------------
 *		index_form_tuple
 *
 *		This shouldn't leak any memory; otherwise, callers such as
 *		tuplesort_putindextuplevalues() will be very unhappy.
 * ----------------
 */
IndexTuple
index_form_tuple(TupleDesc tupleDescriptor,
				 Datum *values,
				 bool *isnull)
{
	char	   *tp;				/* tuple pointer */
	IndexTuple	tuple;			/* return tuple */
	Size		size,
				data_size,
				hoff;
	int			i;
	unsigned short infomask = 0;
	bool		hasnull = false;
	uint16		tupmask = 0;
	int			numberOfAttributes = tupleDescriptor->natts;

#ifdef TOAST_INDEX_HACK
	Datum		untoasted_values[INDEX_MAX_KEYS];
	bool		untoasted_free[INDEX_MAX_KEYS];
#endif

	if (numberOfAttributes > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of index columns (%d) exceeds limit (%d)",
						numberOfAttributes, INDEX_MAX_KEYS)));

#ifdef TOAST_INDEX_HACK
	for (i = 0; i < numberOfAttributes; i++)
	{
		Form_pg_attribute att = tupleDescriptor->attrs[i];

		untoasted_values[i] = values[i];
		untoasted_free[i] = false;

		/* Do nothing if value is NULL or not of varlena type */
		if (isnull[i] || att->attlen != -1)
			continue;

		/*
		 * If value is stored EXTERNAL, must fetch it so we are not depending
		 * on outside storage.  This should be improved someday.
		 */
		if (VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			untoasted_values[i] =
				PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)
													  DatumGetPointer(values[i])));
			untoasted_free[i] = true;
		}

		/*
		 * If value is above size target, and is of a compressible datatype,
		 * try to compress it in-line.
		 */
		if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
			VARSIZE(DatumGetPointer(untoasted_values[i])) > TOAST_INDEX_TARGET &&
			(att->attstorage == 'x' || att->attstorage == 'm'))
		{
			Datum		cvalue = toast_compress_datum(untoasted_values[i]);

			if (DatumGetPointer(cvalue) != NULL)
			{
				/* successful compression */
				if (untoasted_free[i])
					pfree(DatumGetPointer(untoasted_values[i]));
				untoasted_values[i] = cvalue;
				untoasted_free[i] = true;
			}
		}
	}
#endif

	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	if (hasnull)
		infomask |= INDEX_NULL_MASK;

	hoff = IndexInfoFindDataOffset(infomask);
#ifdef TOAST_INDEX_HACK
	data_size = heap_compute_data_size(tupleDescriptor,
									   untoasted_values, isnull);
#else
	data_size = heap_compute_data_size(tupleDescriptor,
									   values, isnull);
#endif
	size = hoff + data_size;
	size = MAXALIGN(size);		/* be conservative */

	tp = (char *) palloc0(size);
	tuple = (IndexTuple) tp;

	heap_fill_tuple(tupleDescriptor,
#ifdef TOAST_INDEX_HACK
					untoasted_values,
#else
					values,
#endif
					isnull,
					(char *) tp + hoff,
					data_size,
					&tupmask,
					(hasnull ? (bits8 *) tp + sizeof(IndexTupleData) : NULL));

#ifdef TOAST_INDEX_HACK
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (untoasted_free[i])
			pfree(DatumGetPointer(untoasted_values[i]));
	}
#endif

	/*
	 * We do this because heap_fill_tuple wants to initialize a "tupmask"
	 * which is used for HeapTuples, but we want an indextuple infomask. The
	 * only relevant info is the "has variable attributes" field. We have
	 * already set the hasnull bit above.
	 */
	if (tupmask & HEAP_HASVARWIDTH)
		infomask |= INDEX_VAR_MASK;

	/* Also assert we got rid of external attributes */
#ifdef TOAST_INDEX_HACK
	Assert((tupmask & HEAP_HASEXTERNAL) == 0);
#endif

	/*
	 * Here we make sure that the size will fit in the field reserved for it
	 * in t_info.
	 */
	if ((size & INDEX_SIZE_MASK) != size)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row requires %zu bytes, maximum size is %zu",
						size, (Size) INDEX_SIZE_MASK)));

	infomask |= size;

	/*
	 * initialize metadata
	 */
	tuple->t_info = infomask;
	return tuple;
}



/*
 *
 * Note: infomask doesn't contain tuple size. Caller should set it depending on
 * choosen index tuple type
 */
void
index_form_inmemory_tuple(TupleDesc tupleDescriptor,
						  Datum *values,
						  bool *isnull,
						  InMemoryIndexTuple itup)
{
	Size		size;
	int			i;
	bool		hasnull = false;
	uint16		tupmask = 0;
	int			numberOfAttributes = tupleDescriptor->natts;

#ifdef TOAST_INDEX_HACK
	Datum		untoasted_values[INDEX_MAX_KEYS];
	bool		untoasted_free[INDEX_MAX_KEYS];
#endif

	itup->t_info = 0;

	if (numberOfAttributes > INDEX_MAX_KEYS)
		ereport(ERROR,
				(errcode(ERRCODE_TOO_MANY_COLUMNS),
				 errmsg("number of index columns (%d) exceeds limit (%d)",
						numberOfAttributes, INDEX_MAX_KEYS)));

#ifdef TOAST_INDEX_HACK
	for (i = 0; i < numberOfAttributes; i++)
	{
		Form_pg_attribute att = tupleDescriptor->attrs[i];

		untoasted_values[i] = values[i];
		untoasted_free[i] = false;

		/* Do nothing if value is NULL or not of varlena type */
		if (isnull[i] || att->attlen != -1)
			continue;

		/*
		 * If value is stored EXTERNAL, must fetch it so we are not depending
		 * on outside storage.  This should be improved someday.
		 */
		if (VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			untoasted_values[i] =
				PointerGetDatum(heap_tuple_fetch_attr((struct varlena *)
													  DatumGetPointer(values[i])));
			untoasted_free[i] = true;
		}

		/*
		 * If value is above size target, and is of a compressible datatype,
		 * try to compress it in-line.
		 */
		if (!VARATT_IS_EXTENDED(DatumGetPointer(untoasted_values[i])) &&
			VARSIZE(DatumGetPointer(untoasted_values[i])) > TOAST_INDEX_TARGET &&
			(att->attstorage == 'x' || att->attstorage == 'm'))
		{
			Datum		cvalue = toast_compress_datum(untoasted_values[i]);

			if (DatumGetPointer(cvalue) != NULL)
			{
				/* successful compression */
				if (untoasted_free[i])
					pfree(DatumGetPointer(untoasted_values[i]));
				untoasted_values[i] = cvalue;
				untoasted_free[i] = true;
			}
		}
	}
#endif

	for (i = 0; i < numberOfAttributes; i++)
	{
		if (isnull[i])
		{
			hasnull = true;
			break;
		}
	}

	if (hasnull)
		itup->t_info |= INDEX_NULL_MASK;

#ifdef TOAST_INDEX_HACK
	size = heap_compute_data_size(tupleDescriptor,
								  untoasted_values, isnull);
#else
	size = heap_compute_data_size(tupleDescriptor,
								  values, isnull);
#endif
	itup->t_data = palloc0(size);

	heap_fill_tuple(tupleDescriptor,
#ifdef TOAST_INDEX_HACK
					untoasted_values,
#else
					values,
#endif
					isnull,
					(char *) itup->t_data,
					size,
					&tupmask,
					(hasnull ? (bits8 *) &itup->t_nulls : NULL));

#ifdef TOAST_INDEX_HACK
	for (i = 0; i < numberOfAttributes; i++)
	{
		if (untoasted_free[i])
			pfree(DatumGetPointer(untoasted_values[i]));
	}
#endif

	/*
	 * We do this because heap_fill_tuple wants to initialize a "tupmask"
	 * which is used for HeapTuples, but we want an indextuple infomask. The
	 * only relevant info is the "has variable attributes" field. We have
	 * already set the hasnull bit above.
	 */
	if (tupmask & HEAP_HASVARWIDTH)
		itup->t_info |= INDEX_VAR_MASK;

	/* Also assert we got rid of external attributes */
#ifdef TOAST_INDEX_HACK
	Assert((tupmask & HEAP_HASEXTERNAL) == 0);
#endif

	/*
	 * Here we make sure that the size will fit in the field reserved for it
	 * in t_info.
	 */
	if ((size & INDEX_SIZE_MASK) != size)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("index row requires %zu bytes, maximum size is %zu",
						size, (Size) INDEX_SIZE_MASK)));
}





/* ----------------
 *		nocache_index_getattr
 *
 *		This gets called from index_getattr() macro, and only in cases
 *		where we can't use cacheoffset and the value is not null.
 *
 *		This caches attribute offsets in the attribute descriptor.
 *
 *		An alternative way to speed things up would be to cache offsets
 *		with the tuple, but that seems more difficult unless you take
 *		the storage hit of actually putting those offsets into the
 *		tuple you send to disk.  Yuck.
 *
 *		This scheme will be slightly slower than that, but should
 *		perform well for queries which hit large #'s of tuples.  After
 *		you cache the offsets once, examining all the other tuples using
 *		the same attribute descriptor will go much quicker. -cim 5/4/91
 * ----------------
 */
Datum
nocache_index_getattr(IndexTuple tup,
					  int attnum,
					  TupleDesc tupleDesc)
{
	return nocache_index_getattr_common((char *) tup + IndexInfoFindDataOffset(tup->t_info),
								 (char *) tup + sizeof(IndexTupleData),
								 tup->t_info,
								 attnum,
								 tupleDesc);
}

Datum
im_nocache_index_getattr(InMemoryIndexTuple tup,
						 int attnum,
						 TupleDesc tupleDesc)
{
	return nocache_index_getattr_common((char *) tup->t_data,
								 (tup->t_info & INDEX_NULL_MASK) ? (char *) &tup->t_nulls : NULL,
								 tup->t_info,
								 attnum,
								 tupleDesc);
}

static Datum
nocache_index_getattr_common(char *tuple_data,
							 char *nulls,
							 unsigned infomask,
							 int attnum,
							 TupleDesc tupleDesc)
{
	Form_pg_attribute *att = tupleDesc->attrs;
	char	   *tp;				/* ptr to data part of tuple */
	bits8	   *bp = NULL;		/* ptr to null bitmap in tuple */
	bool		slow = false;	/* do we have to walk attrs? */
	int			off;			/* current offset within data */

	/* ----------------
	 *	 Three cases:
	 *
	 *	 1: No nulls and no variable-width attributes.
	 *	 2: Has a null or a var-width AFTER att.
	 *	 3: Has nulls or var-widths BEFORE att.
	 * ----------------
	 */

	attnum--;

	if (nulls)
	{
		/*
		 * there's a null somewhere in the tuple
		 *
		 * check to see if desired att is null
		 */

		/* XXX "knows" t_bits are just after fixed tuple header! */
		bp = (bits8 *) nulls;

		/*
		 * Now check to see if any preceding bits are null...
		 */
		{
			int			byte = attnum >> 3;
			int			finalbit = attnum & 0x07;

			/* check for nulls "before" final bit of last byte */
			if ((~bp[byte]) & ((1 << finalbit) - 1))
				slow = true;
			else
			{
				/* check for nulls in any "earlier" bytes */
				int			i;

				for (i = 0; i < byte; i++)
				{
					if (bp[i] != 0xFF)
					{
						slow = true;
						break;
					}
				}
			}
		}
	}

	tp = tuple_data;

	if (!slow)
	{
		/*
		 * If we get here, there are no nulls up to and including the target
		 * attribute.  If we have a cached offset, we can use it.
		 */
		if (att[attnum]->attcacheoff >= 0)
		{
			return fetchatt(att[attnum],
							tp + att[attnum]->attcacheoff);
		}

		/*
		 * Otherwise, check for non-fixed-length attrs up to and including
		 * target.  If there aren't any, it's safe to cheaply initialize the
		 * cached offsets for these attrs.
		 */
		if (infomask & INDEX_VAR_MASK)
		{
			int			j;

			for (j = 0; j <= attnum; j++)
			{
				if (att[j]->attlen <= 0)
				{
					slow = true;
					break;
				}
			}
		}
	}

	if (!slow)
	{
		int			natts = tupleDesc->natts;
		int			j = 1;

		/*
		 * If we get here, we have a tuple with no nulls or var-widths up to
		 * and including the target attribute, so we can use the cached offset
		 * ... only we don't have it yet, or we'd not have got here.  Since
		 * it's cheap to compute offsets for fixed-width columns, we take the
		 * opportunity to initialize the cached offsets for *all* the leading
		 * fixed-width columns, in hope of avoiding future visits to this
		 * routine.
		 */
		att[0]->attcacheoff = 0;

		/* we might have set some offsets in the slow path previously */
		while (j < natts && att[j]->attcacheoff > 0)
			j++;

		off = att[j - 1]->attcacheoff + att[j - 1]->attlen;

		for (; j < natts; j++)
		{
			if (att[j]->attlen <= 0)
				break;

			off = att_align_nominal(off, att[j]->attalign);

			att[j]->attcacheoff = off;

			off += att[j]->attlen;
		}

		Assert(j > attnum);

		off = att[attnum]->attcacheoff;
	}
	else
	{
		bool		usecache = true;
		int			i;

		/*
		 * Now we know that we have to walk the tuple CAREFULLY.  But we still
		 * might be able to cache some offsets for next time.
		 *
		 * Note - This loop is a little tricky.  For each non-null attribute,
		 * we have to first account for alignment padding before the attr,
		 * then advance over the attr based on its length.  Nulls have no
		 * storage and no alignment padding either.  We can use/set
		 * attcacheoff until we reach either a null or a var-width attribute.
		 */
		off = 0;
		for (i = 0;; i++)		/* loop exit is at "break" */
		{
			// if (IndexTupleHasNulls(tup) && att_isnull(i, bp))
			if (infomask & INDEX_NULL_MASK && att_isnull(i, bp))
			{
				usecache = false;
				continue;		/* this cannot be the target att */
			}

			/* If we know the next offset, we can skip the rest */
			if (usecache && att[i]->attcacheoff >= 0)
				off = att[i]->attcacheoff;
			else if (att[i]->attlen == -1)
			{
				/*
				 * We can only cache the offset for a varlena attribute if the
				 * offset is already suitably aligned, so that there would be
				 * no pad bytes in any case: then the offset will be valid for
				 * either an aligned or unaligned value.
				 */
				if (usecache &&
					off == att_align_nominal(off, att[i]->attalign))
					att[i]->attcacheoff = off;
				else
				{
					off = att_align_pointer(off, att[i]->attalign, -1,
											tp + off);
					usecache = false;
				}
			}
			else
			{
				/* not varlena, so safe to use att_align_nominal */
				off = att_align_nominal(off, att[i]->attalign);

				if (usecache)
					att[i]->attcacheoff = off;
			}

			if (i == attnum)
				break;

			off = att_addlength_pointer(off, att[i]->attlen, tp + off);

			if (usecache && att[i]->attlen <= 0)
				usecache = false;
		}
	}

	return fetchatt(att[attnum], tp + off);
}

/*
 * Convert an index tuple into Datum/isnull arrays.
 *
 * The caller must allocate sufficient storage for the output arrays.
 * (INDEX_MAX_KEYS entries should be enough.)
 */
void
index_deform_tuple(IndexTuple tup, TupleDesc tupleDescriptor,
				   Datum *values, bool *isnull)
{
	int			i;

	/* Assert to protect callers who allocate fixed-size arrays */
	Assert(tupleDescriptor->natts <= INDEX_MAX_KEYS);

	for (i = 0; i < tupleDescriptor->natts; i++)
	{
		values[i] = index_getattr(tup, i + 1, tupleDescriptor, &isnull[i]);
	}
}

/*
 * Create a palloc'd copy of an index tuple.
 */
IndexTuple
CopyIndexTuple(IndexTuple source)
{
	IndexTuple	result;
	Size		size;

	size = IndexTupleSize(source);
	result = (IndexTuple) palloc(size);
	memcpy(result, source, size);
	return result;
}

/*
 *
 */
IndexTuple
im_index_tuple_to_physical_format(InMemoryIndexTuple itup)
{
	Size		tupsize = im_index_tuple_size(itup);
	IndexTuple	tup = palloc0(tupsize);

	tup->t_tid.ip_blkid = itup->t_tid.ip_blkid;
	tup->t_tid.ip_posid = itup->t_tid.ip_posid;

	tup->t_info = itup->t_info;
	Assert((tupsize & INDEX_SIZE_MASK) != tupsize);
	tup->t_info |= tupsize;

	/* Copy nulls bitmask */
	if (tup->t_info & INDEX_NULL_MASK)
		memcpy((char *) tup + sizeof(IndexTuple),
			   (char *) &itup->t_nulls,
			   sizeof(IndexAttributeBitMapData));

	memcpy((char *) tup + IndexInfoFindDataOffset(tup->t_info),
		   itup->t_data,
		   itup->t_len);

	return tup;
}
