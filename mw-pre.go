package dbmod

import "git.kanosolution.net/kano/kaos"

func MwPreSelectFields(fields ...string) kaos.MWFunc {
	return func(ctx *kaos.Context, payload interface{}) (bool, error) {
		ctx.Data().Set("DbModSelect", fields)
		return true, nil
	}
}
