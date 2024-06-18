package dbmod

import (
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/orm"
	"git.kanosolution.net/kano/kaos"
	"github.com/ariefdarmawan/datahub"
	"github.com/ariefdarmawan/serde"
	"github.com/sebarcode/codekit"
)

type mod struct {
	hubFn func(ctx *kaos.Context) *datahub.Hub
}

var (
	CUDMethods = []string{"save", "delete", "deletemany", "deletemany", "deletequery"}
)

func New() *mod {
	return new(mod)
}

func (m *mod) getHub(ctx *kaos.Context) *datahub.Hub {
	if m.hubFn == nil {
		h, _ := ctx.DefaultHub()
		return h
	}
	return m.hubFn(ctx)
}

func (m *mod) SetHubFn(fn func(ctx *kaos.Context) *datahub.Hub) {
	m.hubFn = fn
}

func (m *mod) Name() string {
	return "sbr-mod-db"
}

func (m *mod) MakeGlobalRoute(svc *kaos.Service) ([]*kaos.ServiceRoute, error) {
	return []*kaos.ServiceRoute{}, nil
}

func (m *mod) registerKxDbHook(model *kaos.ServiceModel) {
	rv := reflect.ValueOf(model.Model)
	rt := rv.Type()
	mtdCount := rt.NumMethod()
	for mtdIdx := 0; mtdIdx < mtdCount; mtdIdx++ {
		mtd := rt.Method(mtdIdx)
		mtdName := mtd.Name

		if !(strings.HasPrefix(mtdName, "Kx") &&
			mtd.Type.NumIn() == 3 && mtd.Type.In(1).String() == "*kaos.Context" &&
			mtd.Type.NumOut() == 1 && mtd.Type.Out(0).String() == "error") {
			continue
		}

		hookName := mtdName[2:]
		if model.HasHook(hookName) {
			continue
		}

		model.RegisterHook(rv.Method(mtdIdx).Interface(), hookName)
	}
}

func (m *mod) MakeModelRoute(svc *kaos.Service, model *kaos.ServiceModel) ([]*kaos.ServiceRoute, error) {
	m.registerKxDbHook(model)

	routes := []*kaos.ServiceRoute{}
	rt := model.ModelType
	alias := model.Name

	var sr *kaos.ServiceRoute
	disabledRoutes := model.DisableRoutes()

	//-- new
	if !codekit.HasMember(disabledRoutes, "new") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "new")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.ResponseType = reflect.PointerTo(reflect.SliceOf(rt))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, parm *codekit.M) (interface{}, error) {
			mdl := reflect.New(rt).Interface().(orm.DataModel)
			model.CallHook("PreNew", ctx, mdl)
			return mdl, nil
		})
		routes = append(routes, sr)
	}

	//-- gets
	if !codekit.HasMember(disabledRoutes, "gets") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "gets")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(dbflex.NewQueryParam())
		sr.ResponseType = reflect.PointerTo(reflect.SliceOf(rt))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, payload *dbflex.QueryParam) (interface{}, error) {
			h := m.getHub(ctx)
			parm := combineQueryParamFromCtx(payload, ctx)

			// setup filter from data's context
			fs := ctx.Data().Get("DBModFilter", []*dbflex.Filter{}).([]*dbflex.Filter)

			// if from http request and has query
			if hr, ok := ctx.Data().Get("http_request", nil).(*http.Request); ok {
				queryValues := hr.URL.Query()
				for k, vs := range queryValues {
					if len(vs) > 0 {
						fs = append(fs, dbflex.Eq(k, vs[0]))
					}
				}
				if len(fs) == 1 {
					parm = combineQueryParam(parm, dbflex.NewQueryParam().SetWhere(fs[0]))
				} else if len(fs) > 1 {
					parm = combineQueryParam(parm, dbflex.NewQueryParam().SetWhere(dbflex.And(fs...)))
				}
			}

			mdl := reflect.New(rt).Interface().(orm.DataModel)
			dest := reflect.New(reflect.SliceOf(rt)).Interface()

			// get data
			e := h.Gets(mdl, parm, dest)
			if e != nil {
				return nil, e
			}

			// get count
			cmd := dbflex.From(mdl.TableName()).Select("_id")
			if parm != nil && parm.Where != nil {
				cmd.Where(parm.Where)
			}

			//cmd.Select("count(*) as RecordCount")
			recordCount := 0
			noCount := payload.Param.GetBool("NoCount")
			if !noCount {
				connIdx, conn, err := h.GetConnection()
				if err == nil {
					defer h.CloseConnection(connIdx, conn)
					recordCount = conn.Cursor(cmd, nil).Count()
				}
			}

			m := codekit.M{}.Set("data", dest).Set("count", recordCount)
			model.CallHook("PostGets", ctx, m)
			return m, nil
		})
		routes = append(routes, sr)
	}

	//-- find
	if !codekit.HasMember(disabledRoutes, "find") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "find")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(new(dbflex.QueryParam))
		sr.ResponseType = reflect.PtrTo(reflect.SliceOf(rt))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, payload *dbflex.QueryParam) (interface{}, error) {
			parm := combineQueryParamFromCtx(payload, ctx)
			mdl := reflect.New(rt).Interface().(orm.DataModel)
			dest := reflect.New(reflect.SliceOf(rt)).Interface()

			// setup filter from data's context
			fs := ctx.Data().Get("DBModFilter", []*dbflex.Filter{}).([]*dbflex.Filter)

			//-- check if it is a http request and has query
			if hr, ok := ctx.Data().Get("http_request", nil).(*http.Request); ok {
				queryValues := hr.URL.Query()
				for k, vs := range queryValues {
					if len(vs) > 0 {
						fs = append(fs, dbflex.Eq(k, vs[0]))
					}
				}
				if len(fs) == 1 {
					parm = combineQueryParam(parm, dbflex.NewQueryParam().SetWhere(fs[0]))
				} else if len(fs) > 1 {
					parm = combineQueryParam(parm, dbflex.NewQueryParam().SetWhere(dbflex.And(fs...)))
				}
			}

			// get data
			h := m.getHub(ctx)
			e := h.Gets(mdl, parm, dest)
			if e != nil {
				return nil, e
			}
			model.CallHook("PostFind", ctx, dest)
			return dest, nil
		})
		routes = append(routes, sr)
	}

	//-- get
	if !codekit.HasMember(disabledRoutes, "get") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "get")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf([]interface{}{})
		sr.ResponseType = reflect.TypeOf(reflect.PtrTo(rt))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, keys []interface{}) (orm.DataModel, error) {
			h := m.getHub(ctx)
			dm := getDataModel(model)

			// build filter
			idFields, _ := dm.GetID(nil)
			filter := []*dbflex.Filter{}
			for idx, idField := range idFields {
				filter = append(filter, dbflex.Eq(idField, keys[idx]))
			}

			// get filter from context
			ctxFilters := ctx.Data().Get("DBModFilter", []*dbflex.Filter{}).([]*dbflex.Filter)
			filter = append(filter, ctxFilters...)

			var e error
			if len(filter) == 1 {
				e = h.GetByFilter(dm, filter[0])
			} else if len(filter) > 1 {
				e = h.GetByFilter(dm, dbflex.And(filter...))
			}
			if e != nil {
				return dm, e
			}

			if ctx.Data().Get(ValidateTag, false).(bool) {
				fn := ctx.Data().Get(ValidateFnTag, func(codekit.M) bool { return false }).(func(codekit.M) bool)
				dm_m, _ := codekit.ToM(dm)
				if !fn(dm_m) {
					return dm, errors.New("validate data error")
				}
			}

			model.CallHook("PostGet", ctx, dm)
			return dm, e
		})
		routes = append(routes, sr)
	}

	//-- save
	if !codekit.HasMember(disabledRoutes, "save") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "save")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(model.Model)
		sr.ResponseType = reflect.TypeOf(model.Model)
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, dm orm.DataModel) (orm.DataModel, error) {
			h := m.getHub(ctx)
			var (
				e  error
				tx *datahub.Hub
			)

			tx, e = h.BeginTx()
			if e != nil {
				tx = h
			}
			defer func() {
				if e == nil {
					tx.Commit()
				} else {
					tx.Rollback()
				}
			}()

			if dmIsNil(dm) {
				return nil, fmt.Errorf("data is nil")
			}

			if ctx.Data().Get(ValidateTag, false).(bool) {
				fn := ctx.Data().Get(ValidateFnTag, func(codekit.M) bool { return false }).(func(codekit.M) bool)
				dm_m, _ := codekit.ToM(dm)
				if !fn(dm_m) {
					return dm, errors.New("validate data error")
				}
				serde.Serde(dm_m, dm)
			}

			if e = model.CallHook("PreSave", ctx, dm); e != nil {
				return dm, e
			}
			fields := ctx.Data().Get("Fields", []string{}).([]string)
			if len(fields) == 0 {
				e = tx.Save(dm)
			} else {
				e = tx.Save(dm, fields...)
			}
			if e != nil {
				return dm, e
			}
			if e = model.CallHook("PostSave", ctx, dm); e != nil {
				return dm, e
			}
			return dm, e
		})
		routes = append(routes, sr)
	}

	//-- insert
	if !codekit.HasMember(disabledRoutes, "insert") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "insert")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(model.Model)
		sr.ResponseType = reflect.TypeOf(model.Model)
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, dm orm.DataModel) (orm.DataModel, error) {
			h := m.getHub(ctx)

			var (
				e  error
				tx *datahub.Hub
			)

			tx, e = h.BeginTx()
			if e != nil {
				tx = h
			}
			defer func() {
				if e == nil {
					tx.Commit()
				} else {
					tx.Rollback()
				}
			}()

			if dmIsNil(dm) {
				return nil, fmt.Errorf("data is nil")
			}

			if e = model.CallHook("PreSave", ctx, dm); e != nil {
				return dm, e
			}
			if e = tx.Insert(dm); e != nil {
				return dm, e
			}
			if e = model.CallHook("PostSave", ctx, dm); e != nil {
				return dm, e
			}
			return dm, e
		})
		routes = append(routes, sr)
	}

	//-- update
	if !codekit.HasMember(disabledRoutes, "update") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "update")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(model.Model)
		sr.ResponseType = reflect.TypeOf(model.Model)
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, dm orm.DataModel) (orm.DataModel, error) {
			h := m.getHub(ctx)

			var (
				e  error
				tx *datahub.Hub
			)

			tx, e = h.BeginTx()
			if e != nil {
				tx = h
			}
			defer func() {
				if e == nil {
					tx.Commit()
				} else {
					tx.Rollback()
				}
			}()

			if dmIsNil(dm) {
				return nil, fmt.Errorf("data is nil")
			}

			if e = model.CallHook("PreSave", ctx, dm); e != nil {
				return dm, e
			}
			fields := ctx.Data().Get("Fields", []string{}).([]string)
			if len(fields) == 0 {
				e = tx.Update(dm)
			} else {
				e = tx.Update(dm, fields...)
			}
			if e != nil {
				return dm, e
			}
			if e = model.CallHook("PostSave", ctx, dm); e != nil {
				return dm, e
			}
			return dm, e
		})
		routes = append(routes, sr)
	}

	//-- updateField
	if !codekit.HasMember(disabledRoutes, "fieldupdate") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "fieldupdate")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(&UpdateFieldRequest{})
		sr.ResponseType = reflect.TypeOf(codekit.M{})
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, payload *UpdateFieldRequest) (codekit.M, error) {
			h := m.getHub(ctx)
			obj := payload.Model
			filters := []*dbflex.Filter{dbflex.Eq("_id", obj.GetString("_id"))}
			ctxFilters := ctx.Data().Get("DBModFilter", []*dbflex.Filter{}).([]*dbflex.Filter)
			if len(ctxFilters) > 0 {
				filters = append(filters, ctxFilters...)
			}
			tableName := model.Model.(orm.DataModel).TableName()
			e := h.UpdateAny(tableName, dbflex.And(filters...), obj, payload.Fields...)
			return obj, e
		})
		routes = append(routes, sr)
	}

	//-- delete
	if !codekit.HasMember(disabledRoutes, "delete") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "delete")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(model.Model)
		//sr.ResponseType = reflect.TypeOf(int(0))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, dm orm.DataModel) (int, error) {
			h := m.getHub(ctx)

			if dmIsNil(dm) {
				return 0, fmt.Errorf("data is nil")
			}

			if ctx.Data().Get(ValidateTag, false).(bool) {
				fn := ctx.Data().Get(ValidateFnTag, func(codekit.M) bool { return false }).(func(codekit.M) bool)
				dm_m, _ := codekit.ToM(dm)
				if !fn(dm_m) {
					return 0, errors.New("validate data error")
				}
			}

			if e := model.CallHook("PreDelete", ctx, dm); e != nil {
				return 0, e
			}

			tx, e := h.BeginTx()
			if e != nil {
				tx = h
			}
			e = tx.Delete(dm)
			if e != nil {
				tx.Rollback()
				return 0, e
			}
			tx.Commit()

			if e := model.CallHook("PostDelete", ctx, dm); e != nil {
				return 0, e
			}
			return 1, nil
		})
		routes = append(routes, sr)
	}

	//-- deletequery
	if !codekit.HasMember(disabledRoutes, "deletequery") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "deletequery")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		sr.RequestType = reflect.TypeOf(new(dbflex.Filter))
		sr.ResponseType = reflect.TypeOf(int(0))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, where *dbflex.Filter) (int, error) {
			h := m.getHub(ctx)

			where = combineFilterFromCtx(where, ctx)
			dm := getDataModel(model)
			if e := model.CallHook("PreDeleteQuery", ctx, where); e != nil {
				return 0, e
			}

			tx, e := h.BeginTx()
			if e != nil {
				tx = h
			}
			e = tx.DeleteQuery(dm, where)
			if e != nil {
				tx.Rollback()
				return 0, e
			}
			tx.Commit()

			if e := model.CallHook("PostDeleteQuery", ctx, where); e != nil {
				return 0, e
			}
			return 1, nil
		})
		routes = append(routes, sr)
	}

	//-- deletemany
	if !codekit.HasMember(disabledRoutes, "deletemany") {
		sr = new(kaos.ServiceRoute)
		sr.Path = filepath.Join(svc.BasePoint(), alias, "deletemany")
		sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
		// sr.RequestType = reflect.TypeOf(new(dbflex.Filter))
		sr.ResponseType = reflect.TypeOf(int(0))
		sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, idValues [][]interface{}) (int, error) {
			h := m.getHub(ctx)

			for _, idValue := range idValues {
				dm := getDataModel(model)
				dm.SetID(idValue...)

				tx, e := h.BeginTx()
				if e != nil {
					tx = h
				}
				e = tx.Delete(dm)
				if e != nil {
					tx.Rollback()
					return 0, e
				}
				tx.Commit()
			}
			if e := model.CallHook("PostDeleteMany", ctx, idValues); e != nil {
				return 0, e
			}
			return 1, nil
		})
		routes = append(routes, sr)
	}

	// queries
	mdl := reflect.New(rt).Interface().(orm.DataModel)
	queries := mdl.Queries()
	for queryName, q := range queries {
		getName := fmt.Sprintf("GetBy" + queryName)
		getsName := fmt.Sprintf("GetsBy" + queryName)
		findName := fmt.Sprintf("FindBy" + queryName)

		if !codekit.HasMember(disabledRoutes, getName) && q.ReturnKind != string(orm.ReturnMulti) {
			sr = new(kaos.ServiceRoute)
			sr.Path = filepath.Join(svc.BasePoint(), alias, getName)
			sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
			sr.RequestType = reflect.TypeOf(codekit.M{})
			sr.ResponseType = reflect.TypeOf(reflect.PtrTo(rt))
			sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, param codekit.M) (orm.DataModel, error) {
				h := m.getHub(ctx)

				dm := getDataModel(model)
				e := h.GetByQuery(dm, queryName, param)
				return dm, e
			})
			routes = append(routes, sr)
		}

		if !codekit.HasMember(disabledRoutes, getsName) && q.ReturnKind != string(orm.ReturnSingle) {
			sr = new(kaos.ServiceRoute)
			sr.Path = filepath.Join(svc.BasePoint(), alias, getsName)
			sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
			sr.RequestType = reflect.TypeOf(codekit.M{})
			sr.ResponseType = reflect.PtrTo(reflect.SliceOf(rt))
			sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, param codekit.M) (interface{}, error) {
				h := m.getHub(ctx)

				mdl := reflect.New(rt).Interface().(orm.DataModel)
				dest := reflect.New(reflect.SliceOf(rt)).Interface()

				e := h.GetsByQuery(mdl, queryName, param, dest)
				if e != nil {
					return nil, e
				}

				connIndex, conn, _ := h.GetConnection()
				defer h.CloseConnection(connIndex, conn)
				recordCount, _ := orm.CountQuery(conn, mdl, queryName, param)

				m := codekit.M{}.Set("data", dest).Set("count", recordCount)
				model.CallHook("PostGets", ctx, m)
				return m, nil
			})
			routes = append(routes, sr)
		}

		if !codekit.HasMember(disabledRoutes, findName) && q.ReturnKind != string(orm.ReturnSingle) {
			sr = new(kaos.ServiceRoute)
			sr.Path = filepath.Join(svc.BasePoint(), alias, findName)
			sr.Path = strings.Replace(sr.Path, "\\", "/", -1)
			sr.RequestType = reflect.TypeOf(codekit.M{})
			sr.ResponseType = reflect.PtrTo(reflect.SliceOf(rt))
			sr.Fn = reflect.ValueOf(func(ctx *kaos.Context, parm codekit.M) (interface{}, error) {
				h := m.getHub(ctx)

				mdl := reflect.New(rt).Interface().(orm.DataModel)
				dest := reflect.New(reflect.SliceOf(rt)).Interface()
				// get data
				e := h.GetsByQuery(mdl, queryName, parm, dest)
				if e != nil {
					return nil, e
				}
				model.CallHook("PostFind", ctx, dest)
				return dest, nil
			})
			routes = append(routes, sr)
		}
	}

	return routes, nil
}

func dmIsNil(dm orm.DataModel) bool {
	return reflect.ValueOf(dm).IsNil()
}

type GetRequest struct {
	Keys []interface{}
}

func getDataModel(sm *kaos.ServiceModel) orm.DataModel {
	return reflect.New(sm.ModelType).Interface().(orm.DataModel)
}
