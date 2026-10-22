package containerstats

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadCPUStatFileV1(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expected    uint64
		expectError bool
	}{
		{
			name:        "valid cpu usage",
			content:     "123456789\n",
			expected:    123456789,
			expectError: false,
		},
		{
			name:        "valid cpu usage without newline",
			content:     "987654321",
			expected:    987654321,
			expectError: false,
		},
		{
			name:        "empty file",
			content:     "",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid content",
			content:     "not_a_number\n",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "cpuacct.usage")
			err := os.WriteFile(tmpFile, []byte(tt.content), 0644)
			assert.NoError(t, err, "failed to create temp file")

			result, err := readCPUStatFileV1(tmpFile)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestReadCPUStatFileV1_FileNotFound(t *testing.T) {
	_, err := readCPUStatFileV1("/nonexistent/path/to/file")
	assert.Error(t, err, "expected error for nonexistent file")
}

func TestReadCPUStatFileV2(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expected    uint64
		expectError bool
	}{
		{
			name:        "valid cpu.stat format",
			content:     "usage_usec 123456789\n",
			expected:    123456789,
			expectError: false,
		},
		{
			name:        "valid cpu.stat with multiple lines",
			content:     "usage_usec 555555\nuser_usec 111111\nsystem_usec 444444\n",
			expected:    555555,
			expectError: false,
		},
		{
			name:        "empty file",
			content:     "",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid format - non-numeric value",
			content:     "usage_usec not_a_number\n",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "cpu.stat")
			err := os.WriteFile(tmpFile, []byte(tt.content), 0644)
			assert.NoError(t, err, "failed to create temp file")

			result, err := readCPUStatFileV2(tmpFile)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestReadCPUStatFileV2_FileNotFound(t *testing.T) {
	_, err := readCPUStatFileV2("/nonexistent/path/to/file")
	assert.Error(t, err, "expected error for nonexistent file")
}

func TestReadMemoryStatFile(t *testing.T) {
	tests := []struct {
		name        string
		content     string
		expected    uint64
		expectError bool
	}{
		{
			name:        "valid memory usage",
			content:     "1073741824\n",
			expected:    1073741824,
			expectError: false,
		},
		{
			name:        "valid memory usage without newline",
			content:     "536870912",
			expected:    536870912,
			expectError: false,
		},
		{
			name:        "zero memory",
			content:     "0\n",
			expected:    0,
			expectError: false,
		},
		{
			name:        "empty file",
			content:     "",
			expected:    0,
			expectError: true,
		},
		{
			name:        "invalid content",
			content:     "invalid\n",
			expected:    0,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "memory.current")
			err := os.WriteFile(tmpFile, []byte(tt.content), 0644)
			assert.NoError(t, err, "failed to create temp file")

			result, err := readMemoryStatFile(tmpFile)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestReadMemoryStatFile_FileNotFound(t *testing.T) {
	_, err := readMemoryStatFile("/nonexistent/path/to/file")
	assert.Error(t, err, "expected error for nonexistent file")
}
