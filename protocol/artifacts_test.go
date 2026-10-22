package protocol

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestArtifactPaths(t *testing.T) {
	for _, tc := range []struct {
		name    string
		resolve func(string) (string, error)
		want    string
	}{
		{"test", func(name string) (string, error) { return TestFilePath("c", "p", name) }, "collection/c/plan/p/input.csv"},
		{"dependency", func(name string) (string, error) { return DependencyFilePath("c", "p", name) }, "collection/c/dependency/p/input.csv"},
		{"engine data", func(name string) (string, error) { return EngineDataPath("c", "p", name, "2") }, "collection/c/plan/p/input.csv/2/input.csv"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, filename := range []string{"input.csv", " ../input.csv ", `dir\input.csv`} {
				got, err := tc.resolve(filename)
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
			for _, filename := range []string{"", ".", "..", "/"} {
				got, err := tc.resolve(filename)
				assert.EqualError(t, err, "invalid filename")
				assert.Empty(t, got)
			}
		})
	}
}
