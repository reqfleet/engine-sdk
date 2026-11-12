package resources

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type samplerSource struct {
	cpu, memory       uint64
	cpuErr, memoryErr error
	at                time.Time
}

func newTestSampler() (*Sampler, *samplerSource) {
	source := &samplerSource{memory: 8192, at: time.Now()}
	return &Sampler{
		readCPUUsage:    func() (uint64, error) { return source.cpu, source.cpuErr },
		readMemoryUsage: func() (uint64, error) { return source.memory, source.memoryErr },
		now:             func() time.Time { return source.at },
	}, source
}

func TestSamplerUsesElapsedTimeAndSkipsBaseline(t *testing.T) {
	sampler, source := newTestSampler()
	sample, err := sampler.Sample()
	require.ErrorIs(t, err, ErrSampleUnavailable)
	assert.Equal(t, Sample{}, sample)

	for _, step := range []struct {
		elapsed time.Duration
		cpu     uint64
		cores   float64
	}{
		{2 * time.Second, 1_000_000, 0.5},
		{250 * time.Millisecond, 750_000, 3},
		{3 * time.Second, 0, 0}, // A successful unchanged counter is a real zero.
		{500 * time.Nanosecond, 1, 2},
	} {
		source.cpu += step.cpu
		source.at = source.at.Add(step.elapsed)
		sample, err = sampler.Sample()
		require.NoError(t, err)
		assert.Equal(t, Sample{CPUCores: step.cores, MemoryBytes: 8192}, sample)
	}
}

func TestSamplerRebaselinesOnResetAndCounterDecrease(t *testing.T) {
	for _, boundary := range []string{"reset", "counter decrease"} {
		t.Run(boundary, func(t *testing.T) {
			sampler, source := newTestSampler()
			source.cpu = 5_000_000
			_, err := sampler.Sample()
			require.ErrorIs(t, err, ErrSampleUnavailable)
			source.at = source.at.Add(time.Second)
			source.cpu += 1_000_000
			sample, err := sampler.Sample()
			require.NoError(t, err)
			if boundary == "reset" {
				sampler.Reset()
				source.cpu += 100_000_000 // Idle usage must not leak into the new run.
			} else {
				source.cpu = 0
			}
			source.at = source.at.Add(10 * time.Second)
			sample, err = sampler.Sample()
			require.ErrorIs(t, err, ErrSampleUnavailable)
			assert.Equal(t, Sample{}, sample)
			source.cpu += 250_000
			source.at = source.at.Add(500 * time.Millisecond)
			sample, err = sampler.Sample()
			require.NoError(t, err)
			assert.Equal(t, 0.5, sample.CPUCores)
		})
	}
}

func TestSamplerReadFailuresDiscardSampleAndPreserveBaseline(t *testing.T) {
	cpuFailure, memoryFailure := errors.New("CPU inaccessible"), errors.New("memory malformed")
	for _, failure := range []struct {
		name              string
		cpuErr, memoryErr error
	}{
		{"CPU", cpuFailure, nil},
		{"memory", nil, memoryFailure},
	} {
		for _, initial := range []bool{true, false} {
			t.Run(failure.name+"/initial="+strconv.FormatBool(initial), func(t *testing.T) {
				sampler, source := newTestSampler()
				source.cpu = 1_000_000
				if !initial {
					_, err := sampler.Sample()
					require.ErrorIs(t, err, ErrSampleUnavailable)
				}
				source.cpu += 1_000_000
				source.at = source.at.Add(time.Second)
				source.cpuErr, source.memoryErr = failure.cpuErr, failure.memoryErr
				sample, err := sampler.Sample()
				require.Error(t, err)
				if failure.cpuErr != nil {
					assert.ErrorIs(t, err, cpuFailure)
					assert.Contains(t, err.Error(), "CPU usage")
				}
				if failure.memoryErr != nil {
					assert.ErrorIs(t, err, memoryFailure)
					assert.Contains(t, err.Error(), "memory usage")
				}
				assert.Equal(t, Sample{}, sample, "failed attempts must not expose partial measurements")
				source.cpuErr, source.memoryErr = nil, nil
				source.at = source.at.Add(time.Second)
				sample, err = sampler.Sample()
				if initial {
					require.ErrorIs(t, err, ErrSampleUnavailable)
					assert.Equal(t, Sample{}, sample)
				} else {
					require.NoError(t, err)
					assert.Equal(t, Sample{CPUCores: 0.5, MemoryBytes: 8192}, sample, "recovery uses the last successful baseline")
				}
			})
		}
	}
}

func TestSamplerRejectsNonpositiveTimeWithoutAdvancingBaseline(t *testing.T) {
	for _, elapsed := range []time.Duration{0, -time.Second} {
		t.Run(elapsed.String(), func(t *testing.T) {
			sampler, source := newTestSampler()
			baseline := source.at
			_, err := sampler.Sample()
			require.ErrorIs(t, err, ErrSampleUnavailable)
			source.cpu = 1_000_000
			source.at = baseline.Add(elapsed)
			sample, err := sampler.Sample()
			require.Error(t, err)
			assert.NotErrorIs(t, err, ErrSampleUnavailable)
			assert.Equal(t, Sample{}, sample)
			source.at = baseline.Add(2 * time.Second)
			sample, err = sampler.Sample()
			require.NoError(t, err)
			assert.Equal(t, 0.5, sample.CPUCores)
		})
	}
}
