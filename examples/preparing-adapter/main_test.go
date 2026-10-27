package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/reqfleet/engine-sdk/agent"
	"github.com/stretchr/testify/require"
)

func TestBeforeStartRejectsOperationBoundaries(t *testing.T) {
	for _, test := range []struct {
		name   string
		option string
	}{
		{name: "bare next", option: "next"},
		{name: "long next", option: "--next"},
		{name: "short next", option: "-:"},
		{name: "bundled next", option: "-sS:"},
		{name: "abbreviated next", option: "--NeX"},
		{name: "whitespace and delimiter", option: " \tNeXt:\r"},
		{name: "bare include", option: "config = \"nested.curl\""},
		{name: "long include", option: "--config nested.curl"},
		{name: "abbreviated include", option: "--Conf nested.curl"},
		{name: "bundled include", option: "-sSKnested.curl"},
		{name: "NUL", option: "ne\x00xt"},
		{name: "after long line", option: "header = \"X-Padding: " + strings.Repeat("x", 70*1024) + "\"\nnext"},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := []byte("url = \"http://127.0.0.1/one\"\n" + test.option + "\nurl = \"http://127.0.0.1/two\"")
			run := agent.RunContext{
				TestFile:  filepath.Join(t.TempDir(), "original.curl"),
				Workspace: agent.Workspace{Temp: t.TempDir()},
				Settings:  json.RawMessage(`{"concurrency":"3","duration":"1","plan_name":"rejected"}`),
			}
			require.NoError(t, os.WriteFile(run.TestFile, source, 0600))

			require.Error(t, beforeStart(context.Background(), run))
			unchanged, err := os.ReadFile(run.TestFile)
			require.NoError(t, err)
			require.Equal(t, source, unchanged, "rejection must not alter the uploaded artifact")
			prepared, err := os.ReadDir(run.Workspace.Temp)
			require.NoError(t, err)
			require.Empty(t, prepared, "rejection must remove both temporary and published output")
		})
	}
}
