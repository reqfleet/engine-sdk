package containerstats

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Sample contains container resource measurements. A false validity flag means
// unavailable, not zero; consumers must omit that measurement, including any
// previously published CPU rate. MemoryBytes includes cgroup-accounted cache.
type Sample struct {
	CPUCores    float64
	CPUValid    bool
	MemoryBytes uint64
	MemoryValid bool
}

// Sampler converts cumulative cgroup CPU microseconds into average CPU cores.
// Its zero value is ready to use with ReadCPUUsage, ReadMemoryUsage and time.Now.
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

// Reset discards the CPU baseline. The next successful sample reports memory
// but no CPU rate, even if the cgroup counter has not reset between runs.
func (s *Sampler) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cpuMicros, s.cpuAt, s.cpuStarted = 0, time.Time{}, false
}

// Sample reads both resources synchronously. The first successful sample and a
// decreasing CPU counter establish a baseline without producing a CPU rate.
// Subsequent rates use actual elapsed monotonic time, not a polling interval.
// A nonpositive interval returns an error without changing the baseline.
//
// Either read failing leaves the CPU baseline unchanged and CPUValid false.
// Memory is independently valid whenever its read succeeds, even on a CPU error
// or the first sample. Errors identify each failing resource and wrap the reader
// errors (including both when both fail). Callers may consume valid memory while
// handling the error; a complete-exposition exporter may instead fail the scrape.
func (s *Sampler) Sample() (Sample, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	readCPU, readMemory, now := s.readCPUUsage, s.readMemoryUsage, s.now
	if readCPU == nil {
		readCPU = ReadCPUUsage
	}
	if readMemory == nil {
		readMemory = ReadMemoryUsage
	}
	if now == nil {
		now = time.Now
	}
	cpuMicros, cpuErr := readCPU()
	at := now()
	memoryBytes, memoryErr := readMemory()
	sample := Sample{MemoryBytes: memoryBytes, MemoryValid: memoryErr == nil}
	if cpuErr != nil {
		cpuErr = fmt.Errorf("CPU usage: %w", cpuErr)
	}
	if memoryErr != nil {
		memoryErr = fmt.Errorf("memory usage: %w", memoryErr)
	}
	if cpuErr != nil || memoryErr != nil {
		return sample, errors.Join(cpuErr, memoryErr)
	}
	if !s.cpuStarted || cpuMicros < s.cpuMicros {
		s.cpuMicros, s.cpuAt, s.cpuStarted = cpuMicros, at, true
		return sample, nil
	}
	elapsed := at.Sub(s.cpuAt)
	if elapsed <= 0 {
		return sample, errors.New("CPU sampling interval must be positive")
	}
	// Preserve sub-microsecond elapsed precision rather than truncating Duration.
	sample.CPUCores = float64(cpuMicros-s.cpuMicros) / (float64(elapsed) / float64(time.Microsecond))
	sample.CPUValid = true
	s.cpuMicros, s.cpuAt = cpuMicros, at
	return sample, nil
}
