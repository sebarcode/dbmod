package dbmod

import (
	"reflect"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/kaos"
)

const (
	QueryParamTag = "mdb_query_parm"
	ValidateTag   = "mdb_validate"
	ValidateFnTag = "mdb_validate_fn"
)

func combineQueryParamFromCtx(origin *dbflex.QueryParam, ctx *kaos.Context) *dbflex.QueryParam {
	if origin == nil {
		origin = dbflex.NewQueryParam()
	}

	if origin.Where != nil {
		filterString2Date(origin.Where)
	}

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
	origin = filterString2Date(origin)
	other = filterString2Date(other)

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

func filterString2Date(f *dbflex.Filter) *dbflex.Filter {
	if f == nil {
		return f
	}

	if f.Op == dbflex.OpAnd || f.Op == dbflex.OpOr {
		for index, itemF := range f.Items {
			itemF = filterString2Date(itemF)
			f.Items[index] = itemF
		}
		return f
	}

	vf := reflect.ValueOf(f.Value)
	if vf.Kind() == reflect.Ptr {
		vf = vf.Elem()
	}

	if vf.Kind() == reflect.String {
		if dt, err := time.Parse(time.RFC3339, vf.String()); err == nil {
			f.Value = dt
		}
	}

	return f
}
