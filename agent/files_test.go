// Derived from reqfleet/shibuya; modified for engine-sdk.
package agent

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHandler(t *testing.T) {
	root := "/tes-agent"
	h := NewAgentDirHandler(root)
	testcases := []struct {
		name     string
		expected string
		maker    func(sub ...string) string
	}{
		{
			name:     "agent folder",
			expected: root,
			maker: func(sub ...string) string {
				return string(h.Dir())
			},
		},
		{
			name:     "conf folder",
			expected: path.Join(root, "/test-conf"),
			maker: func(sub ...string) string {
				return string(h.ConfFilesDir())
			},
		},
		{
			name:     "test files folder",
			expected: path.Join("", "/test-data"),
			maker: func(sub ...string) string {
				return string(h.TestFilesDir())
			},
		},
		{
			name:     "result files folder",
			expected: path.Join(root, "/test-result"),
			maker: func(sub ...string) string {
				return string(h.ResultFilesDir())
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, tc.maker())
		})
	}
}

func TestJoin(t *testing.T) {
	parent := "/root"
	subdirs := []string{"a", "b", "c"}
	assert.Equal(t, "/root/a/b/c", join(parent, subdirs...))
}
