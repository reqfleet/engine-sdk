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
// diagnostics, and negative coordinates. Diagnostic ordering is preserved.
// A Go error represents an operational failure or cancellation, not rejection of
// an invalid artifact. Callers must check both the error and the result contract.
//
// A valid result does not grant upload admission or execution capability and says
// nothing about label counts, runtime cardinality safety, or artifact rewriting.
// Operator policy that skips validation must not be represented as Valid. Engine
// registration, capability discovery, invocation, upload safeguards, tier policy,
// and preparation of stored content belong to the host, not this package.
package validation
