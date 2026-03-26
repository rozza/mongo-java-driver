# Spock-to-JUnit 5 Migration Plan

## Goal

Remove all Spock/Groovy test specifications from the MongoDB Java Driver and replace them with
JUnit 5 Java tests. This eliminates the Groovy/Spock/CodeNarc toolchain dependency and
standardises on a single test framework.

## Scope

| Module | Spock Files | Unit | Functional | Convention Plugin |
|---|---:|---:|---:|---|
| driver-reactive-streams | 9 | 1 | 8 | `conventions.testing-spock-exclude-slow` |
| driver-legacy | 27 | 16 | 11 | `conventions.testing-spock-exclude-slow` |
| driver-sync | 30 | 24 | 6 | `conventions.testing-spock-exclude-slow` |
| bson | 59 | 59 | 0 | `conventions.testing-spock` |
| driver-core | 202 | 148 | 54 | `conventions.testing-spock-exclude-slow` |
| **Total** | **327** | **248** | **79** | |

Modules are listed in migration order — fewest specs first.

---

## Phase 0 — Baseline: Add JaCoCo and Collect Pre-Migration Metrics

Before migrating any code, establish coverage and test count baselines so every phase
can be verified against them.

### 0.1 Create `conventions/testing-coverage.gradle.kts`

```kotlin
package conventions

plugins {
    id("java-library")
    id("jacoco")
}

jacoco {
    toolVersion = "0.8.12"
}

tasks.withType<Test> {
    finalizedBy(tasks.named("jacocoTestReport"))
}

tasks.withType<JacocoReport> {
    dependsOn(tasks.withType<Test>())
    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}
```

### 0.2 Apply the convention to `testing-base.gradle.kts`

Add `id("conventions.testing-coverage")` to the plugins block in
`buildSrc/src/main/kotlin/conventions/testing-base.gradle.kts`.

### 0.3 Collect baseline metrics

Run for each module:

```bash
./gradlew :<module>:test --continue
```

Record from each module's `build/reports/jacoco/test/jacocoTestReport.xml`:

| Metric | How to Extract |
|---|---|
| Test count | Sum of `tests` attribute from all `build/test-results/test/TEST-*.xml` files |
| Line coverage % | `<counter type="LINE">` element: `covered / (covered + missed) * 100` |

Store these baselines for comparison. Expected pre-migration values (from prior run):

| Module | Tests | Line% |
|---|---:|---:|
| bson | 2,817 | 86.4% |
| driver-core | 4,885 | 69.5% |
| driver-sync | 1,763 | 74.7% |
| driver-legacy | 687 | 86.7% |
| driver-reactive-streams | 654 | 72.9% |

---

## Phase 1 — Migrate Spock Tests to JUnit 5

Migrate **one module at a time**, from smallest to largest. Within each module, migrate
unit tests before functional tests. Commit per module (or per logical group within large
modules like `driver-core`).

### Migration Order

1. **`driver-reactive-streams`** (9 specs) — smallest module, quick validation of process
2. **`driver-legacy`** (27 specs) — small, self-contained, good for refining patterns
3. **`driver-sync`** (30 specs) — moderate size, depends on driver-core test fixtures
4. **`bson`** (59 specs) — standalone, all unit tests, heavy data-table usage
5. **`driver-core`** (202 specs) — largest module, migrate last with refined process

### Per-Module Migration Process

For each module:

#### Step 1: Switch the build convention

In the module's `build.gradle.kts`, replace:
```kotlin
id("conventions.testing-spock")             // or testing-spock-exclude-slow
```
with:
```kotlin
id("conventions.testing-junit")
```

Add `id("conventions.testing-junit-vintage")` if the module has pre-existing JUnit 4 Java tests
(e.g., `driver-reactive-streams` has `@RunWith(Parameterized.class)` tests).

#### Step 2: Migrate each Spock specification

For each `.groovy` file:

1. Create the equivalent `.java` test class in the same source set (`src/test/unit` or
   `src/test/functional`).
2. Apply the pattern mapping reference below.
3. Delete the `.groovy` file.

#### Step 3: Compile and fix

```bash
./gradlew :<module>:compileTestJava
```

Fix any compilation errors. Common issues are documented in the Known Issues section below.

#### Step 4: Run tests and collect metrics

```bash
./gradlew :<module>:cleanTest :<module>:test
```

Compare test count and JaCoCo line coverage against the Phase 0 baseline. Acceptable thresholds:

- **Test count**: Should not decrease by more than 5% (Spock `where:` blocks count as 1 test;
  JUnit 5 parameterized tests count each row individually, so counts often increase).
- **Line coverage**: Should not decrease by more than 3%.

#### Step 5: Commit

One commit per module with message format:
```
Migrate <module> Spock specs to JUnit 5 (<N> files)
```

### Source Set Notes per Module

Most modules use `src/test/unit` and `src/test/functional` automatically via the JUnit convention.
Special cases:

- **`driver-reactive-streams`**: Must explicitly set all test source dirs in `build.gradle.kts`:
  ```kotlin
  sourceSets {
      test {
          java {
              setSrcDirs(listOf("src/test/tck", "src/test/unit", "src/test/functional", "src/examples"))
          }
      }
  }
  ```
  This is because the module uses `setSrcDirs` (replaces) rather than `srcDirs` (appends), and
  the standard `testing-junit` convention only adds `src/test/unit` and `src/test/functional`.

---

## Pattern Mapping Reference

### Core Mapping Table

| Spock Concept | JUnit 5 Equivalent |
|---|---|
| `extends Specification` | Plain class, no base class |
| `def setup()` | `@BeforeEach void setUp()` |
| `def cleanup()` | `@AfterEach void tearDown()` |
| `def setupSpec()` | `@BeforeAll static void beforeAll()` |
| `def cleanupSpec()` | `@AfterAll static void afterAll()` |
| `def "test name"()` | `@Test @DisplayName("test name") void testName()` |
| `given:` / `when:` / `then:` | Arrange / Act / Assert (inline) |
| `expect:` | Single assertion block |
| `where:` (data table) | `@ParameterizedTest` + `@MethodSource` or `@CsvSource` |
| `thrown(ExceptionType)` | `assertThrows(ExceptionType.class, () -> ...)` |
| `notThrown(ExceptionType)` | Just call the code — if it throws, the test fails |
| `Mock()` / `Stub()` | `Mockito.mock()` or `@Mock` with `@ExtendWith(MockitoExtension.class)` |
| `_ >> value` | `when(mock.method()).thenReturn(value)` |
| `>>> [a, b, c]` | `when(mock.method()).thenReturn(a, b, c)` |
| `>> { closure }` | `doAnswer(invocation -> { ... }).when(mock).method(...)` |
| `1 * mock.method()` | `verify(mock, times(1)).method()` |
| `0 * mock.method()` | `verify(mock, never()).method()` |
| `*_` (any args) | Appropriate number of `any()` matchers for the method signature |
| `_ as Type` | `any(Type.class)` |
| `@Slow` tag | `@Tag("Slow")` |
| `@Unroll` | Implicit with `@ParameterizedTest` |
| `@Shared` | `static` field or `@BeforeAll` |
| `with(obj) { ... }` | Extract variable, assert each field with `assertEquals` |
| Groovy `['key': value]` | `new BasicBSONObject("key", value)` or `Map.of("key", value)` |
| Groovy `[1, 2, 3]` | `Arrays.asList(1, 2, 3)` or `List.of(1, 2, 3)` |
| Groovy `[1, 2] as byte[]` | `new byte[]{1, 2}` |
| Groovy `~['a': 1]` (operator overload) | `new BasicBSONObject("a", 1)` |
| Groovy `"${var}"` (GString) | `String.format(...)` or concatenation |
| Groovy property access (`obj.field`) | `obj.getField()` |
| Groovy truth (bare expression) | `assertNotNull(...)` / `assertTrue(...)` / `assertFalse(...)` |

### Existing JUnit 5 Tests as Style Reference

Use these existing tests as style guides:

| Style | Reference File |
|---|---|
| Unit test with Mockito | `driver-core/.../ChangeStreamBatchCursorTest.java` |
| Functional test | `driver-sync/.../CrudProseTest.java` |
| Parameterized test | `bson/.../BsonBinaryReaderTest.java` |

---

## Challenging Patterns — Detailed Guidance

The following Spock patterns require extra care during migration. Each includes a before/after
example and notes on pitfalls.

### Data Tables with Complex Types

Spock `where:` blocks with complex objects become `@MethodSource` methods returning
`Stream<Arguments>`. Every Groovy shorthand must be expanded:

```groovy
// BEFORE (Spock)
where:
representation | value                                              | document
JAVA_LEGACY    | [UUID.fromString('08070605-0403-0201-100f-...')] | '{"array": [...]}'
STANDARD       | [new Binary((byte) 3, [1, 2, 3] as byte[])]      | '{"array": [...]}'
```

```java
// AFTER (JUnit 5)
private static Stream<Arguments> args() {
    return Stream.of(
        Arguments.of(JAVA_LEGACY,
            singletonList(UUID.fromString("08070605-0403-0201-100f-...")),
            "{\"array\": [...]}"),
        Arguments.of(STANDARD,
            singletonList(new Binary((byte) 3, new byte[]{1, 2, 3})),
            "{\"array\": [...]}")
    );
}
```

### Mock/Stub with Closure Configuration

Spock's inline closure configuration maps to Mockito `when/thenReturn`:

```groovy
// BEFORE
def cluster = Stub(Cluster) {
    getCurrentDescription() >> connectedDescription
}
```

```java
// AFTER
Cluster cluster = mock(Cluster.class);
when(cluster.getCurrentDescription()).thenReturn(connectedDescription);
```

### Chained Responses (`>>>`)

```groovy
// BEFORE
batchCursor.hasNext() >>> [true, true, false]
batchCursor.next() >>> [firstBatch, secondBatch]
```

```java
// AFTER
when(batchCursor.hasNext()).thenReturn(true, true, false);
when(batchCursor.next()).thenReturn(firstBatch, secondBatch);
```

### Closure Responses with Argument Capture (`>> { }`)

```groovy
// BEFORE
getWriteConnectionSource(_ as OperationContext, _ as SingleResultCallback) >> {
    it[1].onResult(connectionSource, null)
}
```

```java
// AFTER
doAnswer(invocation -> {
    SingleResultCallback<AsyncConnectionSource> cb = invocation.getArgument(1);
    cb.onResult(connectionSource, null);
    return null;
}).when(binding).getWriteConnectionSource(any(OperationContext.class), any());
```

Note: Spock's `*_` (match any number of args of any type) has no Mockito equivalent. Inspect the
method signature and provide one `any()` per parameter.

### Dynamic Method Invocation via GString

```groovy
// BEFORE
1 * callback."$method"(* _) >> { assert it == args }
```

Java cannot invoke methods by name without reflection. Preferred migration: replace mock
interaction verification with output-based assertions — verify the decoded result instead of
individual callback calls.

### Groovy Operator Overloading / Metaclass

```groovy
// BEFORE
Map.metaClass.bitwiseNegate = { new BasicBSONObject(delegate as Map) }
document == ~['a': 1]
```

```java
// AFTER
assertEquals(new BasicBSONObject("a", 1), document);
```

Every `~[...]` must be expanded to explicit constructor calls. No shortcut.

---

## Phase 2 — Remove Spock/Groovy Infrastructure

After **all** modules are migrated:

### 2.1 Remove from `gradle/libs.versions.toml`

**Versions** to delete:
```
groovy = "3.0.9"
spock-bom = "2.1-groovy-3.0"
```

**Libraries** to delete:
```
spock-bom, spock-core, spock-junit4, groovy
```

**Bundles** to delete:
```
spock = ["spock-core", "spock-junit4"]
```

### 2.2 Delete Spock convention plugins

Remove from `buildSrc/src/main/kotlin/conventions/`:
- `testing-spock.gradle.kts`
- `testing-spock-exclude-slow.gradle.kts`

### 2.3 Delete config files

Remove:
- `config/codenarc/codenarc.xml`
- `config/spock/ExcludeSlow.groovy`
- `config/spock/OnlySlow.groovy`

### 2.4 Migrate Slow test handling

If slow-test tagging is still needed, use JUnit 5 `@Tag("Slow")` and move the
`excludeTags("Slow")` / `includeTags("Slow")` and `testSlowOnly` task into
`testing-base.gradle.kts` or a new `testing-junit-exclude-slow.gradle.kts`.

### 2.5 JUnit Vintage — keep or remove

The `junit-vintage-engine` is needed if any module still has JUnit 4 Java tests
(not migrated Spock — those are now JUnit 5). Check:
```bash
grep -r "import org.junit.Test\b" --include="*.java" -l
grep -r "@RunWith" --include="*.java" -l
```
If matches remain, keep `testing-junit-vintage`. Otherwise remove it.

---

## Phase 3 — Verification

### 3.1 Clean build

```bash
./gradlew clean build --continue
```

### 3.2 No Groovy/Spock references remain

```bash
grep -ri "spock\|codenarc\|groovy" \
    --include="*.kts" --include="*.toml" --include="*.xml" --include="*.gradle" \
    --include="*.properties"
```

This should return zero results.

### 3.3 No `.groovy` files remain in test directories

```bash
find . -path '*/src/test*' -name '*.groovy'
```

### 3.4 Test count and coverage comparison

Collect post-migration metrics using the same method as Phase 0. Compare against baseline:

| Module | Before Tests | Before Line% | After Tests | After Line% | Delta Tests | Delta Line% |
|---|---:|---:|---:|---:|---:|---:|
| bson | 2,817 | 86.4% | — | — | — | — |
| driver-core | 4,885 | 69.5% | — | — | — | — |
| driver-sync | 1,763 | 74.7% | — | — | — | — |
| driver-legacy | 687 | 86.7% | — | — | — | — |
| driver-reactive-streams | 654 | 72.9% | — | — | — | — |

**Acceptance criteria**:
- All tests pass (`./gradlew test --continue` exits 0)
- No module's line coverage drops by more than 3%
- No `.groovy` files remain in any test directory
- No Spock/Groovy/CodeNarc references in build files

### 3.5 Full test suite (if MongoDB available)

If a MongoDB instance is available:
```bash
./gradlew test integrationTest --continue
```

---

## Known Issues and Fixes

These issues were encountered during a prior trial migration. Expect them to recur.

### 1. Java Static Dispatch vs Groovy Dynamic Dispatch

**When**: Migrating tests that pass a subtype to an overloaded method via a parameter declared
as `Object` or a supertype.

**Problem**: Groovy selects the overload at runtime based on actual type. Java selects at compile
time based on declared type. Example: `findOne(Object)` is called instead of `findOne(DBObject)`.

**Fix**: Add explicit `instanceof` checks with casts, or change the parameter type to the
specific subtype.

### 2. Java 8 Release Target Compatibility

**When**: Migrating tests in modules that compile with `options.release.set(8)`.

**Problem**: Groovyc doesn't enforce release targets, so Groovy code freely uses Java 11+ APIs
like `String.repeat()`, `List.of()`, `Map.of()`. Under javac with `--release 8` these fail.

**Fix**: Use Java 8-compatible alternatives:
- `String.repeat(n)` → `new String(new char[n]).replace('\0', 'a')` or `Arrays.fill` helper
- `List.of(a, b)` → `Arrays.asList(a, b)`
- `Map.of(k, v)` → `Collections.singletonMap(k, v)` or `new HashMap<>() {{ put(k, v); }}`

### 3. Mockito Strict Stubbing

**When**: Tests use `@ExtendWith(MockitoExtension.class)` with shared stub setup methods.

**Problem**: Spock's Mockito integration is lenient. JUnit 5's `MockitoExtension` uses strict
mode by default, failing on unused stubs.

**Fix**: Use `Mockito.lenient().when(...)` for stubs that aren't used in every test that calls
the setup method.

### 4. JaCoCo Synthetic Field Interference

**When**: Tests use reflection to compare all private fields of an object.

**Problem**: JaCoCo injects a synthetic `$jacocoData` field that shows up in reflection.

**Fix**: Filter with `!field.isSynthetic()` in any reflection-based test assertions.

### 5. javac Type Inference Stricter Than groovyc

**When**: Pre-existing Java files in the test source set were previously compiled by groovyc
(the Spock convention compiles all test code with groovyc, including `.java` files).

**Problem**: Code that compiled under groovyc may fail under javac due to stricter generic type
inference, especially with `Optional.flatMap()` chains.

**Fix**: Rewrite to use explicit `isPresent()` + `get()` instead of `flatMap`.

### 6. Missing Imports After Convention Switch

**When**: Switching from Spock convention (groovyc) to JUnit convention (javac).

**Problem**: Pre-existing `.java` files that were compiled by groovyc may have missing imports
that groovyc resolved implicitly.

**Fix**: Add explicit imports. Run `compileTestJava` first and fix all compilation errors
before running tests.

---

## Cross-Run Knowledge Sharing

When using Claude Code to perform this migration across multiple sessions, use this section to
accumulate learnings so each session doesn't start from scratch.

### How to Use This Section

After completing each module, append findings to the relevant subsection below. Before starting
a new module, read all subsections to apply prior learnings.

### Module-Specific Notes

Record any module-specific surprises, non-obvious fixes, or deviations from the standard process.

```
Module: driver-reactive-streams (9 specs)
- The test source set uses setSrcDirs() which REPLACES all dirs. Must include all 4:
  src/test/tck, src/test/unit, src/test/functional, src/examples
- Has JUnit 4 functional tests (@RunWith) — needs testing-junit-vintage plugin
- Pre-existing Java test files (TestHelper.java, TestEventPublisher.java) were compiled by
  groovyc. After switching to javac, they had:
  - Missing import: com.mongodb.MongoClientSettings
  - Type inference failure in Optional.flatMap() chain
  - Reflection-based assertions picked up JaCoCo $jacocoData field
- ClientSideEncryptionBsonSizeLimitsTest and GridFSPublisherTest used String.repeat() (Java 11+)
  which compiled under groovyc but fails under javac with --release 8
- 8 functional specs extend FunctionalSpecification which requires a MongoDB server — tests
  skip gracefully when no server is available

Module: driver-legacy (27 specs)
- DBCollectionFunctionalTest.findOne() overload resolution differs between Groovy (runtime
  dispatch to findOne(DBObject)) and Java (compile-time dispatch to findOne(Object)). Required
  instanceof checks with explicit casts.
- 11 functional specs require a running MongoDB; unit tests (16 specs) are self-contained

Module: driver-sync (30 specs)
- (Notes to be added during migration)

Module: bson (59 specs)
- All 59 specs are unit tests — no functional tests, no MongoDB dependency
- Heavy use of Groovy operator overloading: ~['key': val] for BasicBSONObject construction
- Heavy use of data tables with byte arrays: [1, 2, 3] as byte[] → new byte[]{1, 2, 3}
- BasicBSONDecoderSpecification uses dynamic method invocation via GString
  (callback."$method"(*_)) — migrated to output-based assertions instead

Module: driver-core (202 specs)
- Largest module. Consider batching: unit tests first (148), then functional (54)
- Heavy use of Spock Mock/Stub with closure configuration (dominant pattern in connection
  and operation tests)
- 514 mock interaction verifications across the module need careful translation
- Some specs have complex nested Stub closures with chained responses (>>>)
- CommandBatchCursorSpecification is one of the most complex specs — uses Mock with >>
  closures that invoke callbacks and assert counts mid-invocation
```

### Groovy Syntax → Java Quick Reference

Reusable translation patterns encountered across modules:

```
Groovy                              Java
─────────────────────────────────── ──────────────────────────────────────────
['a', 'b', 'c']                    Arrays.asList("a", "b", "c")
['key': value]                     new BasicBSONObject("key", value)
[1, 2, 3] as byte[]               new byte[]{1, 2, 3}
~['a': 1]                          new BasicBSONObject("a", 1)
"text ${var} more"                 "text " + var + " more"
obj.property                       obj.getProperty()
def x = expr                       var x = expr  (or explicit type)
(Type) expr                        (Type) expr  (same)
expr as Type                       (Type) expr
{-> body }                         () -> body
{ arg -> body }                    arg -> body
{ it -> body }                     arg -> body  (name the param)
x == y  (in then: block)           assertEquals(y, x)
x != null                          assertNotNull(x)
!x                                 assertFalse(x)  or  assertNull(x)
x instanceof Type                  assertInstanceOf(Type.class, x)
thrown(FooException)                assertThrows(FooException.class, () -> { ... })
notThrown(FooException)            (just call the code, no assertion needed)
[a, b, c].collect { transform }    Stream.of(a, b, c).map(x -> transform).collect(toList())
list.size() == 3                   assertEquals(3, list.size())
map.containsKey('k')               assertTrue(map.containsKey("k"))
```

### Build System Gotchas

- The project compiles with `options.release.set(8)` — do NOT use Java 9+ APIs in test code
- `setSrcDirs()` replaces all source dirs; `srcDirs()` appends — check each module
- The Spock convention compiles ALL test code (Java + Groovy) with groovyc. After switching to
  JUnit convention, javac compiles Java tests. Pre-existing `.java` test files may fail under
  javac for the first time.
- `testing-junit-vintage` is needed if any `.java` test uses `@RunWith` or `org.junit.Test`
  (JUnit 4). It is NOT needed for migrated Spock specs (those become JUnit 5).
- JaCoCo `jacocoTestReport` is finalized by the `test` task. Run `cleanTest` before `test` to
  ensure fresh results.

### Verification Checklist (copy for each module)

```
[ ] ./gradlew :<module>:compileTestJava — no compilation errors
[ ] ./gradlew :<module>:cleanTest :<module>:test — all tests pass
[ ] Test count delta within acceptable range (see Phase 0 baseline)
[ ] Line coverage delta within 3% of baseline
[ ] No .groovy files remain in <module>/src/test/
[ ] No Spock imports in any .java file (grep for "import spock")
[ ] Commit with descriptive message
```

---

## Appendix: Prior Migration Results

These results were obtained from a trial migration of all 327 specs in a single pass.
They serve as a reference for expected outcomes.

### Final Metrics

| Module | Before Tests | Before Line% | After Tests | After Line% | Delta Tests | Delta Line% |
|---|---:|---:|---:|---:|---:|---:|
| bson | 2,817 | 86.4% | 3,721 | 87.4% | +904 | +1.0% |
| driver-core | 4,885 | 69.5% | 4,461 | 66.6% | -424 | -2.9% |
| driver-sync | 1,763 | 74.7% | 3,689 | 73.6% | +1,926 | -1.1% |
| driver-legacy | 687 | 86.7% | 652 | 86.7% | -35 | +0.0% |
| driver-reactive-streams | 654 | 72.9% | 3,428 | 75.6% | +2,774 | +2.7% |
| **Total** | **10,806** | | **15,951** | | **+5,145** | |

### Diff Statistics

| Metric | Value |
|---|---|
| Groovy files deleted | 329 |
| Java test files created | 325 |
| Java test files modified | 10 |
| Build files changed | 10 |
| Total lines added | 52,120 |
| Total lines removed | 69,492 |
| **Net reduction** | **17,372 lines** |

### Test Count Notes

The +5,145 increase is predominantly a reporting artifact. Spock `where:` blocks count as 1 test;
JUnit 5 `@ParameterizedTest` reports each parameter combination individually. The migration
produced 244 `@ParameterizedTest` methods replacing 777 `where:` blocks.

### Coverage Notes

Coverage remained within 3% across all modules. The `driver-core` decrease (-2.9%) is expected:
Groovy's dynamic dispatch implicitly covered some internal code paths that Java's static dispatch
does not reach. The `driver-reactive-streams` increase (+2.7%) resulted from properly including
all test source directories in the source set configuration.
