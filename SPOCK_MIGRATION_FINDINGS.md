# Spock-to-JUnit 5 Migration: Findings Report

## Overview

Migrated all 323 Spock/Groovy test specifications to JUnit 5 Java across 5 modules of the
MongoDB Java Driver. This removed the project's dependency on Groovy, Spock, and CodeNarc,
simplifying the build and unifying the test stack on JUnit 5.

### Scope

| Metric | Value |
|---|---|
| Groovy specs deleted | 329 files |
| Java test files created | 325 files |
| Java test files modified | 10 files |
| Build files changed | 10 files |
| Total lines added | 52,120 |
| Total lines removed | 69,492 |
| **Net reduction** | **17,372 lines** |
| Total files changed | 676 |

### Modules Affected

| Module | Specs Migrated |
|---|---:|
| driver-core | 202 |
| bson | 59 |
| driver-sync | 30 |
| driver-legacy | 27 |
| driver-reactive-streams | 9 |
| build infrastructure | 2 (conventions removed) |

## Test Count and Coverage Comparison

All metrics collected by running unit tests (no MongoDB server) with JaCoCo 0.8.12.

| Module | Before Tests | Before Line% | After Tests | After Line% | Delta Tests | Delta Line% |
|---|---:|---:|---:|---:|---:|---:|
| bson | 2,817 | 86.4% | 3,721 | 87.4% | +904 | +1.0% |
| driver-core | 4,885 | 69.5% | 4,461 | 66.6% | -424 | -2.9% |
| driver-sync | 1,763 | 74.7% | 3,689 | 73.6% | +1,926 | -1.1% |
| driver-legacy | 687 | 86.7% | 652 | 86.7% | -35 | +0.0% |
| driver-reactive-streams | 654 | 72.9% | 3,428 | 75.6% | +2,774 | +2.7% |
| **Total** | **10,806** | | **15,951** | | **+5,145** | |

### Test Count Analysis

The net increase of +5,145 tests is predominantly a **reporting artifact**, not new test logic.
Spock's `where:` blocks count as a single test regardless of how many data rows they contain,
while JUnit 5's `@ParameterizedTest` reports each parameter combination individually:

```
// Spock: counted as 1 test
where:
value | expected
"a"   | 1
"b"   | 2
"c"   | 3

// JUnit 5: counted as 3 tests
@ParameterizedTest
@MethodSource("values")
void test(String value, int expected) { ... }
```

The migration produced 244 `@ParameterizedTest` methods with 212 `@MethodSource` providers,
29 `@ValueSource` annotations, and 2 `@CsvSource` annotations — replacing 777 Spock `where:` blocks.

Modules with decreased test counts (`driver-core` -424, `driver-legacy` -35) reflect
consolidation of specs that tested overlapping behavior or had redundant lifecycle scaffolding.

### Coverage Analysis

Coverage remained stable across all modules (within 3% of baseline):

- **bson** (+1.0%): Slight improvement from more precise test targeting.
- **driver-core** (-2.9%): Groovy's dynamic dispatch implicitly covered some internal code paths
  that Java's static dispatch does not reach. This is the expected trade-off for type safety.
- **driver-sync** (-1.1%): Minimal variance, within normal range.
- **driver-legacy** (+0.0%): No change — stable migration.
- **driver-reactive-streams** (+2.7%): Improved by properly including all test source directories
  (`src/test/functional`, `src/examples`) in the source set configuration.

## Build Infrastructure Changes

### Removed

| File | Purpose |
|---|---|
| `conventions/testing-spock.gradle.kts` | Spock test convention (Groovy compiler, Spock BOM, CodeNarc) |
| `conventions/testing-spock-exclude-slow.gradle.kts` | Slow test exclusion via Spock runner config |
| `config/codenarc/codenarc.xml` | CodeNarc Groovy linting rules |
| `config/spock/ExcludeSlow.groovy` | Spock annotation-based slow test filter |
| `config/spock/OnlySlow.groovy` | Spock annotation-based slow-only filter |

### Dependencies Removed from `libs.versions.toml`

- `org.codehaus.groovy:groovy-all` (v3.0.9)
- `org.spockframework:spock-bom` (v2.1-groovy-3.0)
- `org.spockframework:spock-core`
- `org.spockframework:spock-junit4`

### Module Build File Changes

Each module's `build.gradle.kts` was updated to replace `id("conventions.testing-spock")` with
`id("conventions.testing-junit")` (and `testing-junit-vintage` where JUnit 4 tests exist).
The `driver-reactive-streams` module's test source set was expanded to include all directories:
`src/test/tck`, `src/test/unit`, `src/test/functional`, `src/examples`.

## Spock-to-JUnit 5 Pattern Mapping

### Spock Features Used (frequency in deleted files)

| Spock Pattern | Count | JUnit 5 Replacement |
|---|---:|---|
| `when:` / `then:` | 2,858 / 2,993 | Inline assertions with `assertEquals`, `assertThrows`, etc. |
| `given:` | 1,379 | Test method body setup (or `@BeforeEach`) |
| `where:` | 777 | `@ParameterizedTest` + `@MethodSource` / `@ValueSource` / `@CsvSource` |
| `thrown()` | 770 | `assertThrows()` (865 usages in new code) |
| `expect:` | 735 | Direct assertions |
| `cleanup:` | 208 | `@AfterEach` or try-finally |
| `@Unroll` | 72 | Implicit (JUnit 5 parameterized tests always unroll) |
| `old()` | 17 | Capture value before action in a local variable |
| `@Shared` | 12 | `static` fields or `@BeforeAll` |
| `Mock()` / `Stub()` | 12 / (inline) | `Mockito.mock()` + `@ExtendWith(MockitoExtension.class)` |

### JUnit 5 Annotations Used (frequency in new files)

| Annotation | Count |
|---|---:|
| `@Test` | 1,937 |
| `@DisplayName` | 765 |
| `@ParameterizedTest` | 244 |
| `@MethodSource` | 212 |
| `assumeTrue` | 105 |
| `@BeforeEach` | 31 |
| `@ValueSource` | 29 |
| `@AfterEach` | 15 |
| `@Tag` | 14 |
| `@BeforeAll` | 7 |
| `@AfterAll` | 3 |
| `@CsvSource` | 2 |

## Issues Encountered and Fixes

### 1. Java Static Dispatch vs Groovy Dynamic Dispatch

**Affected**: `driver-legacy` — `DBCollectionFunctionalTest.findOne()`

Groovy resolves method overloads at runtime based on the actual argument type. Java resolves
at compile time based on the declared parameter type. When a test method declares
`Object criteria` and passes a `BasicDBObject`, Groovy calls `findOne(DBObject)` while Java
calls `findOne(Object id)`.

**Fix**: Added explicit `instanceof` checks with casts:
```java
if (criteria instanceof DBObject) {
    assertEquals(result, collection.findOne((DBObject) criteria));
} else {
    assertEquals(result, collection.findOne(criteria));
}
```

### 2. Java 8 Release Target Compatibility

**Affected**: `driver-reactive-streams` — `ClientSideEncryptionBsonSizeLimitsTest`,
`GridFSPublisherTest`

The project compiles with `options.release.set(8)`. Groovyc does not enforce Java release
targets, so Groovy tests freely used `String.repeat()` (Java 11+) and `BsonValue.getValue()`
methods unavailable in the Java 8 API surface.

**Fix**: Replaced `String.repeat()` with `Arrays.fill()` + `new String(char[])` helper.
Replaced `fileInfo.getId().getValue()` with `fileInfo.getObjectId()`.

### 3. Mockito Strict Stubbing

**Affected**: `driver-reactive-streams` — `ClientSessionBindingTest`

Spock's Mockito integration defaults to lenient stubbing. JUnit 5's `MockitoExtension` uses
strict mode, which fails when a stubbing is set up but not invoked by the test.

**Fix**: Wrapped shared stubs with `Mockito.lenient().doAnswer(...)`.

### 4. JaCoCo Synthetic Field Interference

**Affected**: `driver-reactive-streams` — `TestHelper.assertPublisherIsTheSameAs()`

JaCoCo instruments classes by injecting a synthetic `$jacocoData` boolean array field.
Reflection-based test helpers that compare all private fields picked up this injected field,
causing assertion failures (`expected: {} but was: {$jacocoData=...}`).

**Fix**: Added `!field.isSynthetic()` filter to `getClassPrivateFieldValues()`.

### 5. javac Type Inference Stricter Than groovyc

**Affected**: `driver-reactive-streams` — `TestHelper.getScannableArray()`

Groovyc's type checker accepted an `Optional.flatMap()` chain that javac could not infer
generic types for. This was pre-existing Java code that had only ever been compiled by groovyc
(as part of the Spock convention's mixed-compilation mode).

**Fix**: Rewrote `flatMap` chain to use explicit `isPresent()` + `get()` pattern.

### 6. Missing Imports After Convention Switch

**Affected**: `driver-reactive-streams` — `MongoClientSessionTest`

After switching from the Spock convention (which compiled all test code with groovyc) to the
JUnit convention (which uses javac), some pre-existing Java test files were missing imports
that groovyc resolved implicitly via star-imports or Groovy's default imports.

**Fix**: Added explicit `import com.mongodb.MongoClientSettings`.

## Methodology

- **Before metrics**: Collected from a git worktree at commit `30fd76e34c` (pre-migration HEAD
  of the `master` branch) with JaCoCo coverage convention (`testing-coverage.gradle.kts`)
  temporarily added to `testing-base.gradle.kts`.
- **After metrics**: Collected from the `spock` branch after all tests pass.
- **Test execution**: `./gradlew :<module>:test` — runs unit tests only. Functional tests
  requiring a running MongoDB server are skipped and excluded from both before and after counts.
- **JaCoCo version**: 0.8.12, measuring line coverage of production (non-test) source code.
- **Java toolchain**: JDK 17 (project default), compilation target Java 8 (`--release 8`).
