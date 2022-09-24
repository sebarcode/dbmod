package dbmod

import (
	"github.com/sebarcode/codekit"
)

type UpdateFieldRequest struct {
	Model  codekit.M
	Fields []string
}
