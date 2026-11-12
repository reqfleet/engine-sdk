// Package resources normalizes container resource readings for the SDK runtime
// and adapter exporters. It owns CPU-rate sampling and reset policy; raw cgroup
// access remains in package containerstats. It has no Prometheus dependency.
package resources

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/reqfleet/engine-sdk/containerstats"
)

// ErrSampleUnavailable means that a read established a CPU baseline instead of
// producing a complete measurement. Callers should skip publication without warning.
var ErrSampleUnavailable = errors.New("resource sample unavailable: CPU baseline established")

// Sample contains a complete container resource measurement. CPUCores is an
// average CPU rate; MemoryBytes includes cgroup-accounted cache.
type Sample struct {
	CPUCores    float64
	MemoryBytes uint64
}

// Sampler converts cumulative cgroup CPU microseconds into average CPU cores.
// Its zero value uses containerstats.ReadCPUUsage, containerstats.ReadMemoryUsage
// and time.Now.
// It starts no workers and has no dependency on a metrics registry.
//
// Sample and Reset are safe to call concurrently. Do not copy a Sampler after
// first use. The caller owns sampling frequency, run identity and lifecycle;
// serialize Reset with publishing a sample under its corresponding run identity.
type Sampler struct {
	mu              sync.Mutex
	cpuMicros       uint64
	cpuAt           time.Time
	cpuStarted      bool
	readCPUUsage    func() (uint64, error)
	readMemoryUsage func() (uint64, error)
	now             func() time.Time
}

// Reset discards the CPU baseline. The next successful read only establishes a
// new baseline and produces no sample.
func (s *Sampler) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cpuMicros, s.cpuAt, s.cpuStarted = 0, time.Time{}, false
}

// Sample reads resources synchronously. It returns ErrSampleUnavailable when
// establishing a baseline after first use, Reset, or a decreasing CPU counter.
// That attempt produces no CPU or memory measurement.
// Subsequent samples use actual elapsed monotonic time, not a polling interval.
// A nonpositive interval returns an error without changing the baseline.
//
// Any error returns a zero Sample. Errors other than ErrSampleUnavailable leave
// the CPU baseline unchanged. Callers must not publish failed samples. Read
// errors identify the failing resource and wrap its error.
func (s *Sampler) Sample() (Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	readCPU, readMemory, now := s.readCPUUsage, s.readMemoryUsage, s.now
	if readCPU == nil {
		readCPU = containerstats.ReadCPUUsage
	}
	if readMemory == nil {
		readMemory = containerstats.ReadMemoryUsage
	}
	if now == nil {
		now = time.Now
	}
	cpuMicros, err := readCPU()
	if err != nil {
		return Sample{}, fmt.Errorf("CPU usage: %w", err)
	}
	at := now()
	memoryBytes, err := readMemory()
	if err != nil {
		return Sample{}, fmt.Errorf("memory usage: %w", err)
	}
	if !s.cpuStarted || cpuMicros < s.cpuMicros {
		s.cpuMicros, s.cpuAt, s.cpuStarted = cpuMicros, at, true
		return Sample{}, ErrSampleUnavailable
	}
	elapsed := at.Sub(s.cpuAt)
	if elapsed <= 0 {
		return Sample{}, errors.New("CPU sampling interval must be positive")
	}
	// Preserve sub-microsecond elapsed precision rather than truncating Duration.
	cores := float64(cpuMicros-s.cpuMicros) / (float64(elapsed) / float64(time.Microsecond))
	s.cpuMicros, s.cpuAt = cpuMicros, at
	return Sample{CPUCores: cores, MemoryBytes: memoryBytes}, nil
}
