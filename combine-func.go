package dbmod

import (
	"reflect"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/kaos"
	"github.com/sebarcode/codekit"
)

const (
	QueryParamTag = "mdb_query_parm"
	ValidateTag   = "mdb_validate"
	ValidateFnTag = "mdb_validate_fn"
)

func mToQueryParam(m *codekit.M) *dbflex.QueryParam {
	if m == nil {
		return dbflex.NewQueryParam()
	}

	res := dbflex.NewQueryParam()
	filters := []*dbflex.Filter{}
	for k, v := range *m {
		lowerK := strings.ToLower(k)
		switch lowerK {
		case "select":
			if fields, ok := v.([]string); ok {
				res.SetSelect(fields...)
			}

		case "group":
			if fields, ok := v.([]string); ok {
				res.SetGroupBy(fields...)
			}

		case "sort":
			if fields, ok := v.([]string); ok {
				res.SetSort(fields...)
			}

		case "skip":
			if skip, ok := v.(int); ok {
				res.SetSkip(skip)
			}

		case "take":
			if take, ok := v.(int); ok {
				res.SetTake(take)
			}

		case "where":
			if where, ok := v.(dbflex.Filter); ok {
				filters = append(filters, &where)
			}

		case "aggr":
			if aggrs, ok := v.([]*dbflex.AggrItem); ok {
				res.SetAggr(aggrs...)
			}

		default:
			filters = append(filters, dbflex.Eq(k, v))
		}
	}

	if len(filters) == 1 {
		res.SetWhere(filters[0])
	} else if len(filters) > 1 {
		res.SetWhere(dbflex.And(filters...))
	}

	return res
}

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
