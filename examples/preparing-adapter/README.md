# Public adapter-owned plan preparation

This independent module demonstrates a real curl adapter using only the public
[`engine-sdk` v0.1.1](https://github.com/reqfleet/engine-sdk/tree/v0.1.1)
`agent` and `protocol` APIs. It has no Shibuya dependency, SDK internal imports,
workspace requirement, or `replace` directives. Test assertions use public
`testify`. It demonstrates adapter preparation and the SDK-owned lifecycle, not
standalone authorization, production engine registration, or a new production
engine.

## Preparation contract

The SDK resolves the assigned run settings, initializes its run-local workspace,
stages the original artifacts, and completes any dependency preparation before
calling `BeforeStart`. Only after that hook succeeds does it call `BuildCommand`
and launch the workload. The adapter sets `TestFileName: "original.curl"` and
intentionally does not set `ConfFileName`: the coordinator supplies an original
artifact, not a generated curl configuration.

`beforeStart` requires a JSON object containing positive integer strings for
`concurrency` and `duration`. Duration is in minutes and is checked for overflow
before conversion to seconds. `plan_name` is an optional string; absent means an
empty name. Null values and other types for these recognized fields are rejected.
Unknown fields are ignored, including `rampup`: curl has no ramp-up mapping in
this example. Assignments are already per-agent counts; the adapter does not
allocate or divide them.

Only one curl operation is supported. Preparation rejects `next`, `--next`,
`-:`, their accepted abbreviations and bundled short-option forms. It also
rejects `config`/`--config`/`-K` includes, which could hide further operations.
Operation names inside comments or option values are allowed. Automatic
`.curlrc` loading is disabled so ambient configuration cannot add operations.
The artifact must contain no NUL bytes, and each line must be shorter than
curl's 100 KiB config-line limit in the supplied Debian image.

The hook validates while streaming the original bytes unchanged into an
exclusively created sibling temporary file in `Workspace.Temp`. It appends a
separating newline and:

```text
parallel
parallel-max = <assigned concurrency>
max-time = <duration minutes multiplied by 60>
header = "X-Plan-Name: <assigned plan name>"
```

The scalar options come last, so assigned concurrency and timeout replace the
fixture's conflicting `parallel-max = 1` and `max-time = 5`. curl's `max-time` is
a per-transfer timeout, not a scheduler that repeats this finite three-request
workload for the full duration. The header uses curl configuration quoting, not
shell or Go quoting: quotes and backslashes are escaped and Unicode is preserved.
CR, LF, and NUL are rejected in the name to prevent header injection. Other
uploaded options remain native curl options; header options are additive.
`parallel-max` is an upper bound, not a promise that every transfer starts
simultaneously. The cancellation scenario waits for a real in-flight request,
then verifies that every admitted request ends and no queued work leaks into
the recovery run.

Preparation checks cancellation before work, during the streaming copy, and
before/after publication. Validation, copy, write, close, and rename failures
propagate as startup errors. The file is closed before atomic rename to
`Workspace.Temp/prepared.curl`; failed/canceled preparation removes its generated
output. The original `run.TestFile` is never rewritten. `buildCommand` returns
`curl --disable --config <prepared path>`; the SDK owns launching, stopping,
and reaping curl, rather than the hook starting a subprocess itself.

Inputs, temporary files, and results are reset at the next run initialization.
A completed run's generated file remains available until that reset; it is not
promised permanent retention or an empty temp directory on completion.
`Workspace.Config` and `Workspace.State` are reusable locations and are not used
for generated plans. In SDK v0.1.1 inputs live at `/test-data` independently of
`AGENT_ROOT`; temp and results live under the selected agent root. Preparation
failure is reported through run-scoped status as failed/startup with exit code
`-1`; cancellation prevents launch or stops the running process. A coordinator
start request can succeed before asynchronous preparation fails, so callers must
observe run status.

## Build and dependency boundary

Use Go 1.26.9. From `examples/preparing-adapter` (or a copy of this example
directory), run:

```sh
GOWORK=off go build ./...
GOWORK=off go list -tags=integration -deps -test -json ./... |
  jq -e -s 'all(.[]; (.ImportPath | startswith("github.com/reqfleet/shibuya") | not))'
GOWORK=off go list -m -json all |
  jq -e -s 'any(.[]; .Path == "github.com/reqfleet/engine-sdk" and .Version == "v0.1.1") and all(.[]; .Replace == null)'
docker build -t engine-sdk-preparing-adapter:local .
docker run --rm --network bridge engine-sdk-preparing-adapter:local
```

This is a separate Go module pinned to the released SDK, not to the parent
checkout. Root-level `go test ./...` does not include this module. No local
`replace` or workspace is needed.

With Go 1.26.9, Make, and a running Docker daemon, run both the SDK race tests
and this Docker conformance harness from the repository root:

```sh
make test
```

Use `make test-sdk` or `make test-conformance` to run either suite separately.
The [`pr-test` GitHub Actions workflow](../../.github/workflows/pr-test.yml)
runs the same `make test` target on pull requests.

## Real runtime conformance

Run the integration test only in the disposable container: it uses root to bind
its ephemeral-CA HTTPS artifact server to `127.0.0.1:443`, a TLS command peer on
`:2416`, the SDK HTTP server on `:8080`, and an actual HTTP workload target on
`:18080`. Do not publish ports or mount host directories. The image runs the
preparation unit tests, then builds the adapter and integration-tagged test
binary with Go 1.26.9 and `CGO_ENABLED=0`. It installs curl and CA certificates
in a Debian runtime. Its exact entrypoint is:

```sh
/usr/local/bin/conformance.test -test.run='^TestPublicAdapterRuntime$' \
  -test.v -test.count=1 -test.timeout=120s
```

The harness starts `/usr/local/bin/preparing-adapter` once with a test-owned
`AGENT_ROOT`, generated coordinator CA PEM, API key, and public engine metadata.
It communicates over the real TLS subscription using public `protocol.Topic`
and `protocol.Payload`, serves authenticated artifacts at public protocol paths,
and observes authenticated run status and actual target requests. It does not
mock or replace SDK lifecycle methods.

The same child covers a successful three-URL run with concurrency `13`, duplicate
start suppression, a second run with changed name/concurrency `3`, invalid
settings, cancellation while artifact download is blocked, cancellation while
curl is in flight, rejection of multiple operations before any workload launch,
and recovery. It also checks that operation names in native header values are
preserved and an ambient `.curlrc` cannot add requests. Rejected preparation
leaves the original artifact unchanged and no temporary or published config.
Assertions compare generated assignments, request names/counts, and terminal
SDK statuses. The five-component engine name and offset `7` select the
assignment at `runs["7"]`; no engine-specific coordinator transformation is needed.
