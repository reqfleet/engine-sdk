// Package validation defines the public Go contract for upload-time artifact
// validation. The API is versioned with engine-sdk releases; it is not a new wire
// protocol. JSON tags describe result fields, not an invocation transport.
//
// The hosting API compiles and trusts validators. They run synchronously in its
// process, without a sandbox, independent memory quota, or forced Go-parser
// timeout. Adding an adapter requires rebuilding that API. Validators must only
// parse or compile uploaded artifacts, never execute uploaded tests, and must
// honor context cancellation where possible. Artifact.Content remains owned by
// the caller and must not be mutated or retained by a validator.
//
// Valid has no diagnostics. Invalid requires at least one diagnostic with a
// nonempty code and message. Unsupported indicates that validation is unavailable
// for the artifact; it is distinct from success and may explain the limitation
// in diagnostics. Diagnostic coordinates are one-based when nonzero; zero means
// unspecified. Result.Check rejects unknown or empty statuses, malformed
// diagnostics, negative coordinates, and malformed analysis. Diagnostic ordering
// is preserved. A Go error represents an operational failure or cancellation,
// including an analyzer failure, not rejection of an invalid artifact. Do not
// downgrade operational failures to unsupported or dynamic analysis. Callers must
// check both the error and the result contract.
//
// A Validator can return Result.Analysis alongside validation in the same
// invocation, reusing parsed content. The two outcomes remain distinct: successful
// validation does not imply that an exact count is available. The callback takes
// no tier or limit input and returns facts; the host applies admission policy.
// Invalid and Unsupported validation results must not carry analysis.
//
// LabelAnalysis counts engine-defined entities identified by Unit, for example
// "tasks", "test labels", or "request labels". A Locust static task/decorator count
// is not a count of emitted request labels. AnalysisExact requires a nonnegative
// Count and a nonblank Unit; zero means an exact zero count. AnalysisDynamic means
// the entities depend on execution, AnalysisUnsupported means the analyzer cannot
// determine a count, and AnalysisNotApplicable means this analysis does not apply
// to the engine's artifact. These non-exact states require Count to be zero, but
// that value asserts no count; Unit is optional. Nil Analysis means unavailable or
// not supplied, never an exact zero. JSON includes count even when it is zero;
// consumers must interpret it together with status.
//
// A valid result or exact preflight count does not grant upload admission or
// execution capability, prove runtime cardinality safety, or rewrite artifacts.
// Runtime still enforces a distinct-label cap, including dynamically named labels.
// Operator policy that skips validation must not be represented as Valid. Hosts
// may check analysis separately with LabelAnalysis.Check when validation is
// skipped; an empty validation status is not a public Validator outcome. Engine
// registration, capability discovery, invocation, upload safeguards, tier policy,
// and preparation of stored content belong to the host, not this package.
package validation
