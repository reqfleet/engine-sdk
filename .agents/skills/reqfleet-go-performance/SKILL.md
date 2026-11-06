---
name: reqfleet-go-performance
description: Go performance patterns including efficient string handling, type conversions, container capacity hints, and avoiding unnecessary pointers and GC overhead. Use when optimizing Go code or writing performance-critical sections.
---

# Go Performance Patterns

## Repository Environment

Read the target repository's agent instructions and relevant development skills before running Go tools. Use its prescribed execution environment, module working directory, benchmarks, and checks. Do not assume a particular operating system, VM, repository layout, or Make target.

> **Source**: [Uber Go Style Guide: Performance](https://github.com/uber-go/guide/blob/master/style.md#performance)

Performance-specific guidelines apply only to the **hot path**. Don't prematurely optimize—focus these patterns where they matter most.

Benchmark figures and speedups below are illustrative examples retained from the original guide, not guarantees or measurements of the target repository. Measure the relevant workload with its Go version before claiming an improvement.

---

## Prefer strconv over fmt

When converting primitives to/from strings, `strconv` is faster than `fmt`.

> **Source**: [Uber Go Style Guide: Prefer strconv over fmt](https://github.com/uber-go/guide/blob/master/style.md#prefer-strconv-over-fmt)

**Bad:**

```go
for i := 0; i < b.N; i++ {
    s := fmt.Sprint(rand.Int())
}
```

**Good:**

```go
for i := 0; i < b.N; i++ {
    s := strconv.Itoa(rand.Int())
}
```

**Benchmark comparison:**

| Approach | Speed | Allocations |
|----------|-------|-------------|
| `fmt.Sprint` | 143 ns/op | 2 allocs/op |
| `strconv.Itoa` | 64.2 ns/op | 1 allocs/op |

---

## Avoid Repeated String-to-Byte Conversions

Do not create byte slices from a fixed string repeatedly. Instead, perform the conversion once and capture the result.

> **Source**: [Uber Go Style Guide: Avoid repeated string-to-byte conversions](https://github.com/uber-go/guide/blob/master/style.md#avoid-repeated-string-to-byte-conversions)

**Bad:**

```go
for i := 0; i < b.N; i++ {
    w.Write([]byte("Hello world"))
}
```

**Good:**

```go
data := []byte("Hello world")
for i := 0; i < b.N; i++ {
    w.Write(data)
}
```

**Benchmark comparison:**

| Approach | Speed |
|----------|-------|
| Repeated conversion | 22.2 ns/op |
| Single conversion | 3.25 ns/op |

The good version is **~7x faster** because it avoids allocating a new byte slice on each iteration.

---

## Prefer Specifying Container Capacity

Specify container capacity where possible to allocate memory up front. This minimizes subsequent allocations from copying and resizing as elements are added.

> **Source**: [Uber Go Style Guide: Prefer Specifying Container Capacity](https://github.com/uber-go/guide/blob/master/style.md#prefer-specifying-container-capacity)

### Map Capacity Hints

Provide capacity hints when initializing maps with `make()`.

```go
make(map[T1]T2, hint)
```

**Note**: Unlike slices, map capacity hints do not guarantee complete preemptive allocation—they approximate the number of hashmap buckets required.

**Bad:**

```go
files, _ := os.ReadDir("./files")

m := make(map[string]os.DirEntry)
for _, f := range files {
    m[f.Name()] = f
}
// Map resizes dynamically, causing multiple allocations
```

**Good:**

```go
files, _ := os.ReadDir("./files")

m := make(map[string]os.DirEntry, len(files))
for _, f := range files {
    m[f.Name()] = f
}
// Map is right-sized at initialization, fewer allocations
```

### Slice Capacity

Provide capacity hints when initializing slices with `make()`, particularly when appending.

```go
make([]T, length, capacity)
```

Unlike maps, slice capacity is **not a hint**—the compiler allocates exactly that much memory. Subsequent `append()` operations incur zero allocations until capacity is reached.

**Bad:**

```go
for n := 0; n < b.N; n++ {
    data := make([]int, 0)
    for k := 0; k < size; k++ {
        data = append(data, k)
    }
}
```

**Good:**

```go
for n := 0; n < b.N; n++ {
    data := make([]int, 0, size)
    for k := 0; k < size; k++ {
        data = append(data, k)
    }
}
```

**Benchmark comparison:**

| Approach | Time (100M iterations) |
|----------|------------------------|
| No capacity | 2.48s |
| With capacity | 0.21s |

The good version is **~12x faster** due to zero reallocations during append.

---

## Prefer Values; Use Pointers Only When Necessary

> **Sources**: Go Wiki CodeReviewComments (Advisory); [Go GC Guide](https://go.dev/doc/gc-guide)

Prefer passing, returning, and storing values. Use pointers only when required by semantics or justified by measured copy costs—not merely to save a few bytes or because a struct might grow later.

Avoid unnecessary pointers to reduce indirection and potentially avoid heap allocations and GC scanning work. Do not assume every pointer allocates or every value stays on the stack: allocation depends on compiler escape analysis and usage, and values can contain pointers internally.

**Common instances where values should be passed directly:**

- Pointer to a string (`*string`) — strings are already small fixed-size headers
- Pointer to an interface value (`*io.Reader`) — interfaces are fixed-size (type + data pointers)

**Bad:**

```go
func process(s *string) {
	fmt.Println(*s)  // only dereferences, never modifies
}
```

**Good:**

```go
func process(s string) {
	fmt.Println(s)
}
```

**Use pointers when necessary for:**
- Mutating the caller's value or preserving shared identity.
- Representing a meaningful `nil` state or satisfying an API/interface that requires a pointer.
- Avoiding copies of types that must not be copied after use, such as `sync.Mutex`.
- Avoiding expensive copies of large values when benchmarks demonstrate a benefit.

Compare `allocs/op` and `B/op` using the relevant benchmarks with `-benchmem`. Inspect compiler escape-analysis diagnostics (`-gcflags='-m=2'`) when investigating heap allocations; measure the target workload before claiming reduced GC overhead.

---

## Quick Reference

| Pattern | Bad | Good | Improvement |
|---------|-----|------|-------------|
| Int to string | `fmt.Sprint(n)` | `strconv.Itoa(n)` | ~2x faster |
| Repeated `[]byte` | `[]byte("str")` in loop | Convert once outside | ~7x faster |
| Map initialization | `make(map[K]V)` | `make(map[K]V, size)` | Fewer allocs |
| Slice initialization | `make([]T, 0)` | `make([]T, 0, cap)` | ~12x faster |
| Small fixed-size args | `*string`, `*io.Reader` | `string`, `io.Reader` | No indirection |
| Pointer usage | Pointers by default | Values unless semantics or measured copy costs require pointers | May reduce heap allocations and GC scanning |

---

## See Also

- For core style principles: [reqfleet-go-style-core](../reqfleet-go-style-core/SKILL.md)
- For naming conventions: [Effective Go: Names](https://go.dev/doc/effective_go#names)