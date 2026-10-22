package containerstats

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	cgroupV2Path = "/sys/fs/cgroup/cgroup.controllers"
)

var (
	cgroupVersion      = ""
	noContentError     = "No content in the file %s"
	cpuByCgroupVersion = map[string]StatsParser{
		"v2": {
			path:   "/sys/fs/cgroup/cpu.stat",
			parser: readCPUStatFileV2,
		},
		"v1": {
			path:   "/sys/fs/cgroup/cpu,cpuacct/cpuacct.usage",
			parser: readCPUStatFileV1,
		},
	}
	memByCgroupVersion = map[string]StatsParser{
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

type StatsParser struct {
	path   string
	parser func(path string) (uint64, error)
}

func (sp StatsParser) readUsage() (uint64, error) {
	return sp.parser(sp.path)
}

func detectCgroupVersion() string {
	if cgroupVersion != "" {
		return cgroupVersion
	}
	if _, err := os.Stat(cgroupV2Path); err == nil {
		cgroupVersion = "v2"
		return cgroupVersion
	}
	cgroupVersion = "v1"
	return cgroupVersion
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

// Read the CPU usage file for cgroup v1
// This file contains a single number representing the CPU usage in nanoseconds
func readCPUStatFileV1(path string) (uint64, error) {
	t, err := readFirstLine(path)
	if err != nil {
		return 0, err
	}
	if t == "" {
		return 0, fmt.Errorf(noContentError, path)
	}
	return strconv.ParseUint(t, 10, 64)
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

func readUsage(m map[string]StatsParser) (uint64, error) {
	cgroupVersion := detectCgroupVersion()
	sp := m[cgroupVersion]
	return sp.readUsage()
}

// Return the raw usage in microseconds
func ReadCPUUsage() (uint64, error) {
	return readUsage(cpuByCgroupVersion)
}

// Return the RSS memory in bytes
func ReadMemoryUsage() (uint64, error) {
	return readUsage(memByCgroupVersion)
}
