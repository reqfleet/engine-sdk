// Package containerstats implements the readers exposed by the public
// github.com/reqfleet/engine-sdk/containerstats package.
package containerstats

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	cgroupV2Path = "/sys/fs/cgroup/cgroup.controllers"
)

var (
	cgroupVersion      = sync.OnceValue(detectCgroupVersion)
	noContentError     = "No content in the file %s"
	cpuByCgroupVersion = map[string]statsParser{
		"v2": {
			path:   "/sys/fs/cgroup/cpu.stat",
			parser: readCPUStatFileV2,
		},
		"v1": {
			path:   "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage",
			parser: readCPUStatFileV1,
		},
	}
	memByCgroupVersion = map[string]statsParser{
		"v2": {
			path:   "/sys/fs/cgroup/memory.current",
			parser: readMemoryStatFile,
		},
		"v1": {
			path:   "/sys/fs/cgroup/memory/memory.usage_in_bytes",
			parser: readMemoryStatFile,
		},
	}
)

type statsParser struct {
	path   string
	parser func(path string) (uint64, error)
}

func (sp statsParser) readUsage() (uint64, error) {
	return sp.parser(sp.path)
}

func detectCgroupVersion() string {
	if _, err := os.Stat(cgroupV2Path); err == nil {
		return "v2"
	}
	return "v1"
}

func readFirstLine(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	return "", fmt.Errorf(noContentError, path)
}

func findValueInFile(path, key string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 2 && parts[0] == key {
			return parts[1], nil
		}
	}
	return "", fmt.Errorf("key %s not found in file %s", key, path)
}

// Convert the cgroup v1 cumulative CPU time from nanoseconds to microseconds.
func readCPUStatFileV1(path string) (uint64, error) {
	t, err := readFirstLine(path)
	if err != nil {
		return 0, err
	}
	if t == "" {
		return 0, fmt.Errorf(noContentError, path)
	}
	nanoseconds, err := strconv.ParseUint(t, 10, 64)
	if err != nil {
		return 0, err
	}
	return nanoseconds / 1000, nil
}

func readCPUStatFileV2(path string) (uint64, error) {
	t, err := findValueInFile(path, "usage_usec")
	if err != nil {
		return 0, err
	}
	if t == "" {
		return 0, fmt.Errorf(noContentError, path)
	}
	return strconv.ParseUint(t, 10, 64)
}

func readMemoryStatFile(path string) (uint64, error) {
	t, err := readFirstLine(path)
	if err != nil {
		return 0, err
	}
	if t == "" {
		return 0, fmt.Errorf(noContentError, path)
	}
	return strconv.ParseUint(t, 10, 64)
}

func readUsage(m map[string]statsParser) (uint64, error) {
	version := cgroupVersion()
	sp := m[version]
	return sp.readUsage()
}

// ReadCPUUsage returns cumulative cgroup CPU time in microseconds.
func ReadCPUUsage() (uint64, error) {
	return readUsage(cpuByCgroupVersion)
}

// ReadMemoryUsage returns current cgroup-accounted memory in bytes.
func ReadMemoryUsage() (uint64, error) {
	return readUsage(memByCgroupVersion)
}
