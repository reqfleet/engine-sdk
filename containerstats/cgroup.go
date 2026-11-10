// Package containerstats reads Linux container CPU and memory usage from cgroup
// v1 or v2 files. It has no dependency on the agent runtime or Prometheus.
//
// The readers measure the cgroup visible at /sys/fs/cgroup, including all
// processes in that cgroup, not an individual engine PID. They assume the
// container's cgroup is mounted at that root; they do not discover host-side
// cgroup paths or nested process membership.
//
// Cgroup v2 uses cpu.stat and memory.current. Cgroup v1 uses
// cpu,cpuacct/cpuacct.usage and memory/memory.usage_in_bytes under the same root.
// The version is detected once, on first use. Both readers are safe for
// concurrent calls, start no background work, and return errors when the files
// cannot be read or parsed, including on systems without these Linux files.
package containerstats

import internalstats "github.com/reqfleet/engine-sdk/internal/containerstats"

// ReadCPUUsage returns cumulative CPU time in microseconds for either cgroup
// version. Sub-microsecond precision in v1 is truncated. This is a counter, not
// a CPU percentage or core count; divide its delta by elapsed wall-clock
// microseconds to obtain average CPU cores consumed.
func ReadCPUUsage() (uint64, error) {
	return internalstats.ReadCPUUsage()
}

// ReadMemoryUsage returns the current cgroup memory usage in bytes. This is
// cgroup-accounted memory, including cache, not process RSS or a memory limit.
func ReadMemoryUsage() (uint64, error) {
	return internalstats.ReadMemoryUsage()
}
