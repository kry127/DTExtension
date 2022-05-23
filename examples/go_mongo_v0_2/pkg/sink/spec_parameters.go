package sink

import (
	"bytes"
	"fmt"
	"encoding/json"

	"github.com/xeipuuv/gojsonschema"
	"golang.org/x/xerrors"
)

// go:embed spec.json
var Specification string

type SpecParameters struct {
	MongoConnectionString string `json:"mongo_connection_string"`
	TLSCertificate string `json:"tls_certificate"`
}

func Validate(jsonParams string) error {
	specLoader := gojsonschema.NewStringLoader(Specification)
	jsonLoader := gojsonschema.NewStringLoader(jsonParams)

	result, err := gojsonschema.Validate(specLoader, jsonLoader)
	if err != nil {
		return xerrors.Errorf("an error occured during validation: %w", err)
	}
	if !result.Valid() {
		var buf bytes.Buffer
		_, _ = fmt.Fprintf(&buf, "JSON parameters are not valid. see errors :\n")
		for _, desc := range result.Errors() {
			_, _ = fmt.Fprintf(&buf, "- %s\n", desc)
		}
		return xerrors.New(buf.String())
	}
	// that's OK!
	return nil
}


func Parse(jsonParams string) (*SpecParameters, error) {
	var result SpecParameters
	err := json.Unmarshal([]byte(jsonParams), &result)
	if err != nil {
		return nil, xerrors.Errorf("cannot unmarshal JSON to parameters: %w", err)
	}
	return &result, nil
}