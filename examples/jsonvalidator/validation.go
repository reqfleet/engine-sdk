// Package jsonvalidator demonstrates an upload validator with only standard
// library and public SDK dependencies. It checks request descriptions without
// sending requests or executing uploaded tests.
package jsonvalidator

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/reqfleet/engine-sdk/validation"
)

type config struct {
	Requests      []request `json:"requests"`
	DynamicLabels bool      `json:"dynamicLabels"`
}

type request struct {
	Method string `json:"method"`
	URL    string `json:"url"`
	Label  string `json:"label"`
}

// Validate accepts a JSON object containing a nonempty requests array, for
// example {"requests":[{"method":"GET","url":"https://example.com","label":"home"}]}.
// Unknown fields, trailing JSON, empty methods, and URLs without an HTTP(S)
// scheme and host are rejected. It never sends requests.
// Static analysis counts distinct labels; an omitted or empty label defaults to
// method + " " + URL. Top-level "dynamicLabels":true declares runtime-generated
// names and returns dynamic analysis instead of claiming an exact static count.
func Validate(ctx context.Context, artifact validation.Artifact) (validation.Result, error) {
	if err := ctx.Err(); err != nil {
		return validation.Result{}, err
	}

	decoder := json.NewDecoder(bytes.NewReader(artifact.Content))
	decoder.DisallowUnknownFields()
	var cfg config
	err := decoder.Decode(&cfg)
	if ctxErr := ctx.Err(); ctxErr != nil {
		return validation.Result{}, ctxErr
	}
	if err != nil {
		code := "configuration"
		var syntaxError *json.SyntaxError
		if errors.As(err, &syntaxError) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			code = "syntax"
		}
		return invalid(code, err.Error()), nil
	}

	// Any token after the first document is trailing input, even if that input
	// is itself a valid JSON value. Token avoids decoding an unused second value.
	_, err = decoder.Token()
	if ctxErr := ctx.Err(); ctxErr != nil {
		return validation.Result{}, ctxErr
	}
	if err != io.EOF {
		return invalid("syntax", "expected exactly one JSON object; unexpected trailing input"), nil
	}
	if len(cfg.Requests) == 0 {
		return invalid("configuration", "requests must contain at least one request"), nil
	}

	var diagnostics []validation.Diagnostic
	for i, request := range cfg.Requests {
		if err := ctx.Err(); err != nil {
			return validation.Result{}, err
		}
		if strings.TrimSpace(request.Method) == "" {
			diagnostics = append(diagnostics, validation.Diagnostic{
				Code:    "configuration",
				Message: fmt.Sprintf("requests[%d].method must not be empty", i),
			})
		}
		parsedURL, err := url.Parse(request.URL)
		if err != nil || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") || parsedURL.Hostname() == "" {
			diagnostics = append(diagnostics, validation.Diagnostic{
				Code:    "configuration",
				Message: fmt.Sprintf("requests[%d].url must have an HTTP(S) scheme and host", i),
			})
		}
	}
	if err := ctx.Err(); err != nil {
		return validation.Result{}, err
	}
	if len(diagnostics) != 0 {
		return validation.Result{Status: validation.Invalid, Diagnostics: diagnostics}, nil
	}
	analysis := &validation.LabelAnalysis{
		Status: validation.AnalysisDynamic,
		Unit:   "request labels",
	}
	if !cfg.DynamicLabels {
		labels := make(map[string]struct{}, len(cfg.Requests))
		for _, request := range cfg.Requests {
			if err := ctx.Err(); err != nil {
				return validation.Result{}, err
			}
			label := request.Label
			if label == "" {
				label = request.Method + " " + request.URL
			}
			labels[label] = struct{}{}
		}
		analysis.Status = validation.AnalysisExact
		analysis.Count = len(labels)
	}
	return validation.Result{Status: validation.Valid, Analysis: analysis}, nil
}

func invalid(code, message string) validation.Result {
	return validation.Result{
		Status: validation.Invalid,
		Diagnostics: []validation.Diagnostic{{
			Code:    code,
			Message: message,
		}},
	}
}
