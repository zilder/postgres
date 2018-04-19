/*-------------------------------------------------------------------------
 *
 * pg_operator.h
 *	  definition of the system "operator" relation (pg_operator)
 *
 *
 * Portions Copyright (c) 1996-2018, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/catalog/pg_operator.h
 *
 * NOTES
 *	  The Catalog.pm module reads this file and derives schema
 *	  information.
 *
 *-------------------------------------------------------------------------
 */
#ifndef PG_OPERATOR_H
#define PG_OPERATOR_H

#include "catalog/genbki.h"
#include "catalog/pg_operator_d.h"

#include "catalog/objectaddress.h"
#include "nodes/pg_list.h"

/* ----------------
 *		pg_operator definition.  cpp turns this into
 *		typedef struct FormData_pg_operator
 * ----------------
 */
CATALOG(pg_operator,2617,OperatorRelationId)
{
	/* name of operator */
	NameData	oprname;

	/* OID of namespace containing this oper */
	Oid			oprnamespace BKI_DEFAULT(PGNSP);

	/* operator owner */
	Oid			oprowner BKI_DEFAULT(PGUID);

	/* 'l', 'r', or 'b' */
	char		oprkind BKI_DEFAULT(b);

	/* can be used in merge join? */
	bool		oprcanmerge BKI_DEFAULT(f);

	/* can be used in hash join? */
	bool		oprcanhash BKI_DEFAULT(f);

	/* left arg type, or 0 if 'l' oprkind */
	Oid			oprleft BKI_LOOKUP(pg_type);

	/* right arg type, or 0 if 'r' oprkind */
	Oid			oprright BKI_LOOKUP(pg_type);

	/* result datatype */
	Oid			oprresult BKI_LOOKUP(pg_type);

	/* OID of commutator oper, or 0 if none */
	Oid			oprcom BKI_DEFAULT(0) BKI_LOOKUP(pg_operator);

	/* OID of negator oper, or 0 if none */
	Oid			oprnegate BKI_DEFAULT(0) BKI_LOOKUP(pg_operator);

	/* OID of underlying function */
	regproc		oprcode BKI_LOOKUP(pg_proc);

	/* OID of restriction estimator, or 0 */
	regproc		oprrest BKI_DEFAULT(-) BKI_LOOKUP(pg_proc);

	/* OID of join estimator, or 0 */
	regproc		oprjoin BKI_DEFAULT(-) BKI_LOOKUP(pg_proc);
} FormData_pg_operator;

/* ----------------
 *		Form_pg_operator corresponds to a pointer to a tuple with
 *		the format of pg_operator relation.
 * ----------------
 */
typedef FormData_pg_operator *Form_pg_operator;


extern ObjectAddress OperatorCreate(const char *operatorName,
			   Oid operatorNamespace,
			   Oid leftTypeId,
			   Oid rightTypeId,
			   Oid procedureId,
			   List *commutatorName,
			   List *negatorName,
			   Oid restrictionId,
			   Oid joinId,
			   bool canMerge,
			   bool canHash);

extern ObjectAddress makeOperatorDependencies(HeapTuple tuple, bool isUpdate);

extern void OperatorUpd(Oid baseId, Oid commId, Oid negId, bool isDelete);

#endif							/* PG_OPERATOR_H */
// <<<<<<<
// =======
// DESCR("not equal");
// DATA(insert OID = 2062 (  "<"	   PGNSP PGUID b f f 1114 1114	 16 2064 2065 timestamp_lt scalarltsel scalarltjoinsel ));
// DESCR("less than");
// #define OID_TS_LE_TS_OP 2062
// DATA(insert OID = 2063 (  "<="	   PGNSP PGUID b f f 1114 1114	 16 2065 2064 timestamp_le scalarlesel scalarlejoinsel ));
// DESCR("less than or equal");
// #define OID_TS_LEE_TS_OP 2063
// DATA(insert OID = 2064 (  ">"	   PGNSP PGUID b f f 1114 1114	 16 2062 2063 timestamp_gt scalargtsel scalargtjoinsel ));
// DESCR("greater than");
// #define OID_TS_GR_TS_OP 2064
// DATA(insert OID = 2065 (  ">="	   PGNSP PGUID b f f 1114 1114	 16 2063 2062 timestamp_ge scalargesel scalargejoinsel ));
// DESCR("greater than or equal");
// #define OID_TS_GRE_TS_OP 2065
// DATA(insert OID = 2066 (  "+"	   PGNSP PGUID b f f 1114 1186 1114  2553 0 timestamp_pl_interval - - ));
// DESCR("add");
// DATA(insert OID = 2067 (  "-"	   PGNSP PGUID b f f 1114 1114 1186  0	0 timestamp_mi - - ));
// >>>>>>>
