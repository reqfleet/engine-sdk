// Package metricschema defines the metric names and ordered labels of the
// shibuya-v1 metrics contract without registering or depending on collectors.
package metricschema

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
