// Derived from reqfleet/shibuya; modified for engine-sdk.
package protocol

import "path"

const ArtifactPathPrefix = "/coordinator/files"

func TestFilePath(collectionID, planID, filename string) (string, error) {
	name, err := SanitizeFilename(filename)
	if err != nil {
		return "", err
	}
	return path.Join("collection", collectionID, "plan", planID, name), nil
}

func DependencyFilePath(collectionID, planID, filename string) (string, error) {
	name, err := SanitizeFilename(filename)
	if err != nil {
		return "", err
	}
	return path.Join("collection", collectionID, "dependency", planID, name), nil
}

func EngineDataPath(collectionID, planID, filename, engineID string) (string, error) {
	name, err := SanitizeFilename(filename)
	if err != nil {
		return "", err
	}
	return path.Join("collection", collectionID, "plan", planID, name, engineID, name), nil
}
