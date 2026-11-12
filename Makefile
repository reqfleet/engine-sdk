.PHONY: test test-sdk test-validation test-conformance test-toolkit

test: test-sdk test-conformance test-toolkit

test-sdk:
	GOWORK=off go test -race -count=1 ./...

test-validation:
	GOWORK=off go test -race -count=1 ./validation ./examples/jsonvalidator

test-conformance:
	docker build -t engine-sdk-preparing-adapter:conformance examples/preparing-adapter
	docker run --rm --network bridge engine-sdk-preparing-adapter:conformance

test-toolkit:
	docker build -f conformance/Dockerfile -t engine-sdk-toolkit:conformance .
	docker run --rm --network none engine-sdk-toolkit:conformance
