// Package validation defines the public Go contract for optional, best-effort
// upload checks. Validation and label counting provide early user feedback.
// Runtime safety requires separate enforcement. The API is versioned with
// engine-sdk releases. JSON tags describe data fields; invocation transport
// remains separate.
//
// The hosting API compiles and trusts validators. They run synchronously in its
// process, without a sandbox, independent memory quota, or forced Go-parser
// timeout. Adding an adapter requires rebuilding that API. Validators must only
// parse or compile uploaded artifacts, never execute uploaded tests, and must
// honor context cancellation where possible. Artifact.Content remains owned by
// the caller and must not be mutated or retained by a validator.
//
// A Validator accepts a context, Artifact, and Options. Options independently
// suppress checks: SkipPlanFileValidation skips validation without disabling
// counting; SkipLabelCounting skips counting without disabling validation.
// Adapters must honor both flags. Each enabled check remains best-effort.
// An adapter may return Skipped when validation is unavailable for the artifact,
// or omit LabelCount when it cannot determine a count.
//
// Valid means validation succeeded. Skipped means validation was not performed,
// whether disabled by Options or unavailable in the adapter. Neither status may
// carry diagnostics; both may carry an optional LabelCount. Invalid requires at
// least one diagnostic with a nonempty code and message, and must not carry a
// count. Skipping validation must not be reported as Valid.
//
// Diagnostic coordinates are one-based when nonzero; zero means unspecified.
// Diagnostic ordering is preserved. Result.Check rejects unknown or empty
// statuses, malformed diagnostics, negative coordinates, and malformed counts.
// A zero Result is malformed. Return operational failures and cancellation as Go errors.
// Use results for invalid artifacts and unavailable best-effort counts.
// Callers must check both the error and the result contract.
//
// Result.LabelCount is optional. A supplied LabelCount requires a nonnegative
// Count and a nonblank Unit; its Check method validates those fields. Nil means
// no count was supplied and never blocks an upload on counting grounds.
// JSON omits a nil label_count and includes count when it is zero. There are no
// count-completeness statuses.
//
// Unit identifies engine-defined entities, such as "tasks", "test labels", or
// "request labels". Configured JMeter sampler labels and Locust static task
// counts describe preflight entities. Emitted request-label cardinality is
// determined at runtime. The callback returns any available count to its hosting
// control-plane caller, which applies upload policy. It accepts no tier or
// label-limit input.
//
// These checks do not grant execution capability, prove runtime cardinality
// safety, or rewrite artifacts. SDK result-file ingestion and explicitly enabled
// native folding (FoldNativeMetrics) enforce the runtime distinct-label cap,
// including dynamically named labels. Default native proxying streams unchanged
// without SDK admission; native engines enforce their own configured limits.
// The host owns registration, invocation, upload safeguards, mandatory content
// preparation, and tier policy.
//
// The public examples/jsonvalidator adapter demonstrates independent checks:
// counting can run without semantic validation; malformed counting-only input
// and dynamicLabels:true omit the count. Skipping counting preserves validation
// diagnostics, and skipping both checks avoids parsing. Cancellation remains an
// operational error under every option combination.
package validation
