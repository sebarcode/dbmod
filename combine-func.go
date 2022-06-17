package dbmod

import (
	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/kaos"
)

const (
	QueryParamTag = "mdb-query-parm"
	ValidateTag   = "mdb-validate"
	ValidateFnTag = "mdb-validate-fn"
)

func combineQueryParamFromCtx(origin *dbflex.QueryParam, ctx *kaos.Context) *dbflex.QueryParam {
	if ctx.Data().Get(QueryParamTag, nil) != nil {
		other := ctx.Data().Get(QueryParamTag, dbflex.NewQueryParam()).(*dbflex.QueryParam)
		return combineQueryParam(origin, other)
	}

	return origin
}

func combineQueryParam(origin, other *dbflex.QueryParam) *dbflex.QueryParam {
	if origin == nil {
		if other != nil {
			return origin
		}
		return nil
	}

	if other == nil {
		return origin
	}

	if len(other.Aggregates) > 0 {
		origin.Aggregates = append(origin.Aggregates, other.Aggregates...)
	}

	if len(other.GroupBy) > 0 {
		origin.GroupBy = append(origin.GroupBy, other.GroupBy...)
	}

	if len(other.Select) > 0 {
		origin.Select = append(origin.Select, other.Select...)
	}

	if len(other.Sort) > 0 {
		origin.Sort = append(origin.Sort, other.Sort...)
	}

	if other.Skip > 0 {
		origin.Skip = other.Skip
	}

	if other.Take > 0 {
		origin.Take = other.Take
	}

	origin.Where = combineFilter(origin.Where, other.Where)
	return origin
}

func combineFilterFromCtx(origin *dbflex.Filter, ctx *kaos.Context) *dbflex.Filter {
	if ctx.Data().Get(QueryParamTag, nil) != nil {
		other := ctx.Data().Get(QueryParamTag, dbflex.NewQueryParam()).(*dbflex.QueryParam)
		return combineFilter(origin, other.Where)
	}

	return origin
}

func combineFilter(origin, other *dbflex.Filter) *dbflex.Filter {
	if origin == nil {
		if other != nil {
			return other
		}
		return origin
	}

	if other == nil {
		return origin
	}

	return dbflex.And(origin, other)
}
