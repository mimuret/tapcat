package format

import (
	"bytes"
	"fmt"
	"html/template"

	"github.com/mimuret/dtap"
)

type LineFormater struct {
	tpl *template.Template
}

func NewLineFormater(tpl *template.Template) *LineFormater {
	return &LineFormater{tpl: tpl}
}

func (o *LineFormater) Format(dt *dtap.DnstapFlatT) ([]byte, error) {
	b := bytes.NewBuffer(nil)
	err := o.tpl.Execute(b, dt)
	if err != nil {
		return nil, fmt.Errorf("failed to execute template: %w", err)
	}
	return b.Bytes(), nil
}
