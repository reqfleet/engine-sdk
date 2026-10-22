// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import (
	"errors"
	"path"
	"strings"
)

// SanitizeFilename returns a safe base filename suitable for use in storage keys
// and filesystem paths. It prevents path traversal by stripping all path
// components and rejecting empty/dot names.
func SanitizeFilename(input string) (string, error) {
	raw := strings.TrimSpace(input)
	// Normalize Windows-style separators so path.Base can strip path components reliably.
	raw = strings.ReplaceAll(raw, "\\", "/")
	base := path.Base(raw)
	if base == "" || base == "." || base == ".." || base == "/" {
		return "", errors.New("invalid filename")
	}
	return base, nil
}
