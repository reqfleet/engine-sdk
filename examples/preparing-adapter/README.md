# Public adapter-owned plan preparation

This independent module demonstrates curl preparation and the SDK-owned lifecycle.
It uses only public [`engine-sdk` v0.1.1](https://github.com/reqfleet/engine-sdk/tree/v0.1.1)
`agent` and `protocol` APIs, plus `testify` for assertions.
It builds independently of Shibuya, SDK internals, workspaces, and `replace` directives.
The coordinator authorizes execution. Production engine registration remains
separate control-plane work.

## Preparation contract

The SDK resolves assigned settings, initializes the workspace, and stages original
artifacts. It completes dependency preparation before calling `BeforeStart`.
After that hook succeeds, it calls `BuildCommand` and launches the workload.
Set `TestFileName: "original.curl"` and leave `ConfFileName` unset.
The coordinator supplies the original artifact; the adapter generates curl configuration.

`beforeStart` requires a JSON object with positive integer strings for
`concurrency` and `duration`. Duration uses minutes.
Conversion to seconds checks for overflow.
The optional string `plan_name` defaults to empty.
Null and other types are rejected for these fields.
Unknown fields, including `rampup`, are ignored.
Use the assigned per-agent counts unchanged. This example supports a fixed concurrency.

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

Scalar options come last. Assigned concurrency and timeout replace the fixture's
`parallel-max = 1` and `max-time = 5`.
Curl's `max-time` limits each transfer.
The finite three-request workload runs once.
Quote headers using curl configuration syntax: escape quotes and backslashes,
and preserve Unicode. Reject CR, LF, and NUL in names to prevent header injection.
Other uploaded options retain native curl behavior; header options are additive.

`parallel-max` caps concurrent transfers; their start times may differ.
The cancellation scenario waits for a real in-flight request.
It then verifies that every admitted request ends and queued work stays out of
the recovery run.

Preparation checks cancellation before work, during copying, and before and after publication.
Validation, copy, write, close, and rename failures become startup errors.
Close the file before atomically renaming it to `Workspace.Temp/prepared.curl`.
Failed or canceled preparation removes generated output and preserves `run.TestFile`.
`buildCommand` returns `curl --disable --config <prepared path>`.
The SDK launches, stops, and reaps curl.

Inputs, temporary files, and results reset at the next run initialization.
Generated files remain available until that reset.
Keep generated plans in temporary storage; `Workspace.Config` and
`Workspace.State` hold reusable state.
In SDK v0.1.1, inputs live at `/test-data`.
Temp and results live under `AGENT_ROOT`.
Preparation failure reports failed/startup with exit code `-1`.
Cancellation prevents launch or stops the running process.
Observe run status after a start request: asynchronous preparation can still fail.

## Build and dependency boundary

Use the Go version declared in [go.mod](go.mod). From `examples/preparing-adapter`
(or a copy of this example directory), run:

```sh
GOWORK=off go build ./...
GOWORK=off go list -tags=integration -deps -test -json ./... |
  jq -e -s 'all(.[]; (.ImportPath | startswith("github.com/reqfleet/shibuya") | not))'
GOWORK=off go list -m -json all |
  jq -e -s 'any(.[]; .Path == "github.com/reqfleet/engine-sdk" and .Version == "v0.1.1") and all(.[]; .Replace == null)'
docker build -t engine-sdk-preparing-adapter:local .
docker run --rm --network bridge engine-sdk-preparing-adapter:local
```

This separate Go module pins the released SDK and builds independently of the
parent checkout. Run its tests separately from root-level `go test ./...`.

To run SDK race tests and this Docker harness together, use the repository root.
Install the Go version in [go.mod](../../go.mod), Make, and a running Docker daemon:

```sh
make test
```

Use `make test-sdk` or `make test-conformance` to run either suite separately.
The [`pr-test` GitHub Actions workflow](../../.github/workflows/pr-test.yml)
runs the same `make test` target on pull requests.

## Real runtime conformance

Run integration tests only in a disposable container with private ports and storage.
The root test harness binds these services:

- Ephemeral-CA HTTPS artifact server: `127.0.0.1:443`.
- TLS command peer: `:2416`.
- SDK HTTP server: `:8080`.
- HTTP workload target: `:18080`.

The image runs preparation unit tests.
It builds the adapter and integration-tagged test binary with `CGO_ENABLED=0`,
then installs curl and CA certificates in Debian.
Its entrypoint is:

```sh
/usr/local/bin/conformance.test -test.run='^TestPublicAdapterRuntime$' \
  -test.v -test.count=1 -test.timeout=120s
```

The harness starts `/usr/local/bin/preparing-adapter` once with a test-owned
`AGENT_ROOT`, generated coordinator CA PEM, API key, and public engine metadata.
It uses the real TLS subscription with public `protocol.Topic` and `protocol.Payload`.
It serves authenticated artifacts at public protocol paths and observes
authenticated run status and real target requests.

The same child covers:

- A successful three-URL run with concurrency `13` and duplicate-start suppression.
- A second run with a changed name and concurrency `3`.
- Invalid settings and cancellation during artifact download or curl execution.
- Rejection of multiple operations before launch, followed by recovery.
- Preservation of operation names in native header values.
- Isolation from ambient `.curlrc` settings.

Rejected preparation preserves the original artifact and removes temporary and
published config. Assertions compare assignments, request names/counts, and terminal
SDK statuses. The five-component engine name and offset `7` select `runs["7"]`.
The coordinator delivers that assignment directly.
