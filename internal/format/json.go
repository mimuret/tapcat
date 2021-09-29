package format

import (
	"fmt"

	json "github.com/goccy/go-json"

	"github.com/mimuret/dtap"
)

type JsonFormater struct {
}

func NewJsonFormater() *JsonFormater {
	return &JsonFormater{}
}

func (o *JsonFormater) Format(dt *dtap.DnstapFlatT) ([]byte, error) {
	bs, err := json.Marshal(dt)
	if err != nil {
		return nil, fmt.Errorf("failed to make json string: %w", err)
	}
	return bs, nil
}
