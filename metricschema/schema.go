// Package metricschema defines metric names, ordered Prometheus labels, histogram
// boundaries, and request-name admission for shibuya-v1 without depending on collectors.
package metricschema

const (
	// MaxRequestLabelBytes is the inclusive maximum request-label length in bytes,
	// not characters. Labels of 1024 bytes or more are invalid.
	MaxRequestLabelBytes = 1023
	// DefaultLabelLimit is the default number of distinct request names
	// per agent/run when label_limit is unset or empty. Overflow is additional.
	// An explicitly configured zero budget is not replaced by this default.
	DefaultLabelLimit = 10
)

const (
	// RequestLatency is the request latency histogram, measured in milliseconds.
	RequestLatency = "shibuya_latency_label_milliseconds"
	// RequestLatencyBucket is the request latency histogram bucket series.
	RequestLatencyBucket = RequestLatency + "_bucket"
	// RequestLatencySum is the request latency histogram sum series.
	RequestLatencySum = RequestLatency + "_sum"
	// RequestLatencyCount is the request latency histogram count series.
	RequestLatencyCount = RequestLatency + "_count"

	// LLMResponseLatency is the LLM response latency histogram in milliseconds.
	LLMResponseLatency = "shibuya_latency_llm_resp_milliseconds"
	// LLMResponseLatencyBucket is the LLM response latency histogram bucket series.
	LLMResponseLatencyBucket = LLMResponseLatency + "_bucket"
	// LLMResponseLatencySum is the LLM response latency histogram sum series.
	LLMResponseLatencySum = LLMResponseLatency + "_sum"
	// LLMResponseLatencyCount is the LLM response latency histogram count series.
	LLMResponseLatencyCount = LLMResponseLatency + "_count"

	// StatusCounter counts responses by status.
	StatusCounter = "shibuya_status_counter"
	// EgressBytesCounter records response/received bytes under the historical egress name.
	EgressBytesCounter = "shibuya_egress_bytes_counter"
	// ThreadsGauge reports current per-engine concurrency.
	ThreadsGauge = "shibuya_threads_gauge"
	// CPUGauge reports CPU cores consumed by the engine.
	CPUGauge = "shibuya_cpu_gauge"
	// MemoryGauge reports engine memory usage in bytes.
	MemoryGauge = "shibuya_mem_gauge"
)

const (
	// LabelCollectionID identifies the collection.
	LabelCollectionID = "collection_id"
	// LabelPlanID identifies the plan.
	LabelPlanID = "plan_id"
	// LabelRunID identifies the run.
	LabelRunID = "run_id"
	// LabelEngineNo identifies the engine within a run.
	LabelEngineNo = "engine_no"
	// LabelZone identifies the engine's zone.
	LabelZone = "zone"
	// LabelRequest identifies the request or measurement label.
	LabelRequest = "label"
	// LabelStatus identifies the response status.
	LabelStatus = "status"
	// LabelLE identifies the inclusive upper bound of a histogram bucket.
	LabelLE = "le"
)

const (
	// OverflowRequestName is the reserved name for samples exceeding the
	// request-name budget. It does not consume an admission slot.
	OverflowRequestName = "others"
	// LLMResponseLabel selects the LLM response latency histogram.
	LLMResponseLabel = "llm_resp"
	// TTFTLabel identifies time-to-first-token measurements.
	TTFTLabel = "ttft"
)

// LatencyLabels returns a fresh, caller-owned slice of ordered histogram labels.
func LatencyLabels() []string {
	return []string{LabelCollectionID, LabelRequest, LabelRunID, LabelEngineNo, LabelPlanID, LabelZone}
}

// LatencyBucketLabels returns a fresh, caller-owned slice of ordered bucket labels.
func LatencyBucketLabels() []string {
	return []string{LabelCollectionID, LabelRequest, LabelRunID, LabelEngineNo, LabelPlanID, LabelZone, LabelLE}
}

// StatusLabels returns a fresh, caller-owned slice of ordered status counter labels.
func StatusLabels() []string {
	return []string{LabelCollectionID, LabelPlanID, LabelRunID, LabelEngineNo, LabelRequest, LabelZone, LabelStatus}
}

// EgressLabels returns a fresh, caller-owned slice of ordered egress counter labels.
func EgressLabels() []string {
	return []string{LabelCollectionID, LabelPlanID, LabelRunID, LabelEngineNo, LabelRequest, LabelZone}
}

// ThreadsLabels returns a fresh, caller-owned slice of ordered threads gauge labels.
func ThreadsLabels() []string {
	return []string{LabelCollectionID, LabelPlanID, LabelRunID, LabelEngineNo, LabelZone}
}

// ResourceLabels returns a fresh, caller-owned slice of ordered CPU and memory labels.
func ResourceLabels() []string {
	return []string{LabelCollectionID, LabelPlanID, LabelRunID, LabelZone, LabelEngineNo}
}

// RequestLatencyBuckets returns a fresh, caller-owned slice of the ordinary
// request histogram's finite upper bounds in milliseconds, in increasing order.
// Configure collectors once with these bounds, not per observation. Prometheus
// adds the implicit +Inf bucket; exposition validators must append it themselves.
func RequestLatencyBuckets() []float64 {
	return []float64{1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}
}

// LLMResponseLatencyBuckets returns a fresh, caller-owned slice of the LLM
// response histogram's finite upper bounds in milliseconds, in increasing order.
// Configure collectors once with these bounds, not per observation. Prometheus
// adds the implicit +Inf bucket; exposition validators must append it themselves.
func LLMResponseLatencyBuckets() []float64 {
	return []float64{
		100, 250, 500, 1000, 2500, 5000, 7500, 10000, 15000, 20000,
		30000, 45000, 60000, 90000, 120000, 180000, 300000,
	}
}
