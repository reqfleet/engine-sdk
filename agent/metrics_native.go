package agent

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"
	"github.com/reqfleet/engine-sdk/metricschema"
)

// Bound temporary parsing/aggregation memory independently of the named-label budget.
const maxNativeMetricsBytes = 16 << 20

func (as *AgentServer) startMetricsRun(runID string) {
	if as.options.MetricsURL == "" {
		as.labelsLock.Lock()
		as.labelsByRun[runID] = make(map[string]struct{})
		as.labelsLock.Unlock()
	} else if as.options.FoldNativeMetrics {
		as.nativeMetricsLock.Lock()
		as.nativeMetricsRun = runID
		as.nativeLabels = make(map[string]struct{})
		as.nativeMetricsLock.Unlock()
	}
}

func (as *AgentServer) foldNativeMetrics(response *http.Response) error {
	// Preserve upstream failures, including their status and body.
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream returned non-snapshot success status %d", response.StatusCode)
	}
	defer response.Body.Close()
	if encoding := response.Header.Get("Content-Encoding"); encoding != "" && encoding != "identity" {
		return fmt.Errorf("unsupported upstream content encoding %q", encoding)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxNativeMetricsBytes+1))
	if err != nil {
		return fmt.Errorf("reading upstream exposition: %w", err)
	}
	if len(data) > maxNativeMetricsBytes {
		return fmt.Errorf("upstream exposition exceeds %d bytes", maxNativeMetricsBytes)
	}
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("invalid upstream exposition: %w", err)
	}
	if err := checkNativeHistogramSamples(data, families); err != nil {
		return err
	}

	as.nativeMetricsLock.Lock()
	defer as.nativeMetricsLock.Unlock()
	overflow := as.options.NativeMetricsOverflowLabel
	if overflow == "" {
		overflow = labelOthers
	}
	var candidates []string
	for name, family := range families {
		seen := make(map[string]struct{}, len(family.Metric))
		for _, metric := range family.Metric {
			label, err := as.checkNativeMetric(name, family.GetType(), metric)
			if err != nil {
				return fmt.Errorf("invalid upstream metric %s: %w", name, err)
			}
			if label == nil {
				continue
			}
			key := nativeSeriesKey(metric)
			if _, exists := seen[key]; exists {
				return fmt.Errorf("duplicate upstream series in %s", name)
			}
			seen[key] = struct{}{}
			if _, admitted := as.nativeLabels[label.GetValue()]; !admitted && label.GetValue() != overflow {
				candidates = append(candidates, label.GetValue())
			}
		}
	}
	// Admit a stable set over the run, independent of family and exposition order.
	slices.Sort(candidates)
	candidates = slices.Compact(candidates)
	remaining := max(0, as.labelsUsageLimit-len(as.nativeLabels))
	if len(candidates) > remaining {
		candidates = candidates[:remaining]
	}
	var output bytes.Buffer
	names := make([]string, 0, len(families))
	for name := range families {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		family := families[name]
		folded := make(map[string]*dto.Metric)
		metrics := family.Metric[:0]
		for _, metric := range family.Metric {
			label := nativeRequestLabel(metric)
			if label == nil {
				metrics = append(metrics, metric)
				continue
			}
			_, admitted := as.nativeLabels[label.GetValue()]
			_, pending := slices.BinarySearch(candidates, label.GetValue())
			if !admitted && !pending {
				label.Value = &overflow
			}
			key := nativeSeriesKey(metric)
			if aggregate := folded[key]; aggregate != nil {
				if err := mergeNativeMetric(aggregate, metric); err != nil {
					return fmt.Errorf("cannot fold upstream metric %s: %w", name, err)
				}
			} else {
				folded[key] = metric
				metrics = append(metrics, metric)
			}
		}
		family.Metric = metrics
		if _, err := expfmt.MetricFamilyToText(&output, family); err != nil {
			return fmt.Errorf("encoding folded upstream metric %s: %w", name, err)
		}
	}
	// Only a completely valid scrape may consume admission slots. Never retain
	// overflow source names or add cumulative snapshots to an earlier scrape.
	for _, name := range candidates {
		as.nativeLabels[name] = struct{}{}
	}
	response.Body = io.NopCloser(bytes.NewReader(output.Bytes()))
	response.ContentLength = int64(output.Len())
	response.Header.Set("Content-Length", strconv.Itoa(output.Len()))
	response.Header.Set("Content-Type", string(expfmt.NewFormat(expfmt.TypeTextPlain)))
	response.Header.Del("Content-Encoding")
	response.Header.Del("ETag")
	response.Header.Del("Content-MD5")
	return nil
}

func nativeRequestLabel(metric *dto.Metric) *dto.LabelPair {
	for _, label := range metric.Label {
		if label.GetName() == metricschema.LabelRequest {
			return label
		}
	}
	return nil
}

func nativeSeriesKey(metric *dto.Metric) string {
	slices.SortFunc(metric.Label, func(a, b *dto.LabelPair) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	var key strings.Builder
	for _, label := range metric.Label {
		key.WriteString(strconv.Itoa(len(label.GetName())))
		key.WriteByte(':')
		key.WriteString(label.GetName())
		key.WriteString(strconv.Itoa(len(label.GetValue())))
		key.WriteByte(':')
		key.WriteString(label.GetValue())
	}
	return key.String()
}

func (as *AgentServer) checkNativeMetric(name string, kind dto.MetricType, metric *dto.Metric) (*dto.LabelPair, error) {
	request := nativeRequestLabel(metric)
	switch name {
	case metricschema.RequestLatency, metricschema.LLMResponseLatency:
		if request == nil || kind != dto.MetricType_HISTOGRAM {
			return nil, fmt.Errorf("request latency requires a labelled classic histogram")
		}
	case metricschema.StatusCounter, metricschema.EgressBytesCounter:
		if request == nil || kind != dto.MetricType_COUNTER {
			return nil, fmt.Errorf("request totals require a labelled counter")
		}
	}
	if request == nil {
		return nil, nil // Unrelated resource families do not consume request labels.
	}
	if as.nativeMetricsRun == "" {
		return nil, fmt.Errorf("request metrics have no launched run")
	}
	if _, err := (ShibuyaMetric{Label: request.GetValue()}).LabelIsValid(); err != nil {
		return nil, err
	}
	meta := as.options.EngineMeta
	identity := uint8(0)
	status := false
	for _, label := range metric.Label {
		var expected string
		var bit uint8
		switch label.GetName() {
		case metricschema.LabelCollectionID:
			expected, bit = meta.CollectionID, 1
		case metricschema.LabelPlanID:
			expected, bit = meta.PlanID, 2
		case metricschema.LabelRunID:
			expected, bit = as.nativeMetricsRun, 4
		case metricschema.LabelEngineNo:
			expected, bit = meta.EngineID, 8
		case metricschema.LabelZone:
			expected, bit = meta.Zone, 16
		}
		if bit != 0 {
			if label.GetValue() != expected {
				return nil, fmt.Errorf("%s does not match the assigned run", label.GetName())
			}
			identity |= bit
		}
		status = status || label.GetName() == metricschema.LabelStatus
	}
	if identity != 31 {
		return nil, fmt.Errorf("request metric is missing assigned identity labels")
	}
	if name == metricschema.StatusCounter && !status {
		return nil, fmt.Errorf("status counter is missing status")
	}
	if metric.TimestampMs != nil {
		return nil, fmt.Errorf("request snapshots must not supply sample timestamps")
	}
	switch kind {
	case dto.MetricType_COUNTER:
		if metric.Counter == nil || !nativeNonnegative(metric.Counter.GetValue()) {
			return nil, fmt.Errorf("counter must be finite and nonnegative")
		}
	case dto.MetricType_HISTOGRAM:
		if err := checkNativeHistogram(metric.Histogram); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("request label folding requires counters or classic histograms")
	}
	return request, nil
}

func nativeNonnegative(value float64) bool {
	return value >= 0 && !math.IsInf(value, 0) && !math.IsNaN(value)
}

func checkNativeHistogram(histogram *dto.Histogram) error {
	if histogram == nil || histogram.SampleCount == nil || histogram.SampleSum == nil || !nativeNonnegative(histogram.GetSampleSum()) || len(histogram.Bucket) == 0 {
		return fmt.Errorf("histogram requires finite nonnegative sum, count and cumulative buckets")
	}
	var previous uint64
	bound := math.Inf(-1)
	for _, bucket := range histogram.Bucket {
		if bucket.UpperBound == nil || bucket.CumulativeCount == nil || !(bucket.GetUpperBound() > bound) || bucket.GetCumulativeCount() < previous || bucket.GetCumulativeCount() > histogram.GetSampleCount() {
			return fmt.Errorf("histogram buckets must have increasing bounds and cumulative counts")
		}
		bound, previous = bucket.GetUpperBound(), bucket.GetCumulativeCount()
	}
	if !math.IsInf(bound, 1) || previous != histogram.GetSampleCount() {
		return fmt.Errorf("histogram requires a +Inf bucket equal to its count")
	}
	return nil
}

func mergeNativeMetric(target, source *dto.Metric) error {
	if target.Counter != nil && source.Counter != nil {
		total := target.Counter.GetValue() + source.Counter.GetValue()
		if !nativeNonnegative(total) {
			return fmt.Errorf("counter sum overflow")
		}
		*target.Counter.Value = total
		return nil
	}
	a, b := target.Histogram, source.Histogram
	if a == nil || b == nil || len(a.Bucket) != len(b.Bucket) {
		return fmt.Errorf("incompatible histogram buckets")
	}
	if math.MaxUint64-a.GetSampleCount() < b.GetSampleCount() || !nativeNonnegative(a.GetSampleSum()+b.GetSampleSum()) {
		return fmt.Errorf("histogram sum or count overflow")
	}
	for i, bucket := range a.Bucket {
		other := b.Bucket[i]
		if bucket.GetUpperBound() != other.GetUpperBound() {
			return fmt.Errorf("incompatible histogram bucket bounds")
		}
		*bucket.CumulativeCount += other.GetCumulativeCount()
	}
	*a.SampleCount += b.GetSampleCount()
	*a.SampleSum += b.GetSampleSum()
	return nil
}

// expfmt converts histogram counts to uint64 and overwrites repeated sum/count
// samples. Check those raw values too, rather than silently rounding malformed
// input or accepting duplicate cumulative components.
func checkNativeHistogramSamples(data []byte, families map[string]*dto.MetricFamily) error {
	components := make(map[string]int)
	for line := range bytes.Lines(data) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		end := bytes.IndexAny(line, "{ \t")
		if end < 0 {
			continue // The exposition parser diagnoses malformed sample lines.
		}
		name := string(line[:end])
		base := name
		for _, suffix := range []string{"_bucket", "_count", "_sum"} {
			if strings.HasSuffix(name, suffix) {
				base = strings.TrimSuffix(name, suffix)
				break
			}
		}
		family := families[base]
		if family == nil || family.GetType() != dto.MetricType_HISTOGRAM {
			continue
		}
		components[base]++
		if name == base+"_sum" {
			continue
		}
		valueStart := end
		if close := bytes.LastIndexByte(line, '}'); close >= 0 {
			valueStart = close + 1
		}
		fields := bytes.Fields(line[valueStart:])
		if len(fields) == 0 {
			return fmt.Errorf("histogram %s is missing a sample value", base)
		}
		value, err := strconv.ParseFloat(string(fields[0]), 64)
		if err != nil || !nativeNonnegative(value) || value >= math.Exp2(64) || math.Trunc(value) != value {
			return fmt.Errorf("histogram %s counts must be nonnegative uint64 integers", base)
		}
	}
	for name, count := range components {
		expected := 0
		for _, metric := range families[name].Metric {
			expected += len(metric.GetHistogram().GetBucket()) + 2
		}
		if count != expected {
			return fmt.Errorf("histogram %s has missing or duplicate sum/count samples", name)
		}
	}
	return nil
}
