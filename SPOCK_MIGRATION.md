# Spock-to-JUnit 5 Migration

Instructions for Claude Code to migrate all Spock/Groovy test specifications to JUnit 5 Java,
removing the Groovy/Spock/CodeNarc toolchain dependency.

## Execution Order

Process modules in this order (fewest Spock specs first):

| # | Module | Spock Files | Unit | Functional | Notes |
|---|---|---:|---:|---:|---|
| 1 | `driver-reactive-streams` | 9 | 1 | 8 | Has custom `setSrcDirs` and JUnit 4 tests |
| 2 | `driver-legacy` | 27 | 16 | 11 | Has `findOne()` dispatch issue |
| 3 | `driver-sync` | 30 | 24 | 6 | Depends on driver-core test fixtures |
| 4 | `bson` | 59 | 59 | 0 | All unit tests, heavy data tables |
| 5 | `driver-core` | 202 | 148 | 54 | Largest — batch into unit then functional |

---

## Pre-Migration Setup (once, before first module)

### 1. Create `buildSrc/src/main/kotlin/conventions/testing-coverage.gradle.kts`

```kotlin
package conventions

plugins {
    id("java-library")
    id("jacoco")
}

jacoco { toolVersion = "0.8.12" }

tasks.withType<Test> { finalizedBy(tasks.named("jacocoTestReport")) }

tasks.withType<JacocoReport> {
    dependsOn(tasks.withType<Test>())
    reports {
        xml.required.set(true)
        html.required.set(true)
        csv.required.set(false)
    }
}
```

### 2. Add to `buildSrc/src/main/kotlin/conventions/testing-base.gradle.kts`

Add `id("conventions.testing-coverage")` to the plugins block.

### 3. Collect baseline metrics

For each module, run tests and record results:

```bash
./gradlew :<module>:cleanTest :<module>:test --continue
```

Extract metrics using this Python script:

```python
import xml.etree.ElementTree as ET
import os

def collect_metrics(module_path):
    # Test count
    test_dir = f"{module_path}/build/test-results/test/"
    test_count = 0
    test_names = []
    for f in os.listdir(test_dir):
        if f.endswith('.xml'):
            tree = ET.parse(os.path.join(test_dir, f))
            root = tree.getroot()
            test_count += int(root.get('tests', 0))
            for tc in tree.findall('.//testcase'):
                test_names.append(f"{tc.get('classname')}::{tc.get('name')}")

    # Line coverage
    jacoco = f"{module_path}/build/reports/jacoco/test/jacocoTestReport.xml"
    line_pct = 0
    if os.path.exists(jacoco):
        tree = ET.parse(jacoco)
        for counter in tree.getroot().findall('counter'):
            if counter.get('type') == 'LINE':
                missed = int(counter.get('missed'))
                covered = int(counter.get('covered'))
                line_pct = covered / (covered + missed) * 100 if (covered + missed) > 0 else 0

    return test_count, round(line_pct, 1), sorted(test_names)
```

Save baselines per module. Expected values from prior run:

| Module | Tests | Line% |
|---|---:|---:|
| driver-reactive-streams | 654 | 72.9% |
| driver-legacy | 687 | 86.7% |
| driver-sync | 1,763 | 74.7% |
| bson | 2,817 | 86.4% |
| driver-core | 4,885 | 69.5% |

### 4. Extract pre-migration Spock test names

Before modifying any code, extract all Spock test names for later comparison:

```bash
# For each module, extract Spock test names from source
find <module>/src/test -name '*.groovy' -exec \
    sed -n "s/.*def '\([^']*\)'.*/\1/p; s/.*def \"\([^\"]*\)\".*/\1/p" {} \; \
    | sort > /tmp/spock-test-names-<module>.txt

# Also extract Spock class names
find <module>/src/test -name '*Specification.groovy' -exec basename {} .groovy \; \
    | sort > /tmp/spock-classes-<module>.txt
```

### 5. Commit the JaCoCo setup

```
Add JaCoCo coverage convention for Spock migration baseline
```

---

## Per-Module Migration Procedure

Repeat this procedure for each module in order. Do NOT skip steps.
Do NOT begin the next module until the current module has passed the User Acceptance Gate (Step 6).

### Step 1 — Switch build convention

In `<module>/build.gradle.kts`:

**Replace** whichever Spock convention is present:
```kotlin
// REMOVE one of:
id("conventions.testing-spock")
id("conventions.testing-spock-exclude-slow")
```

**Add:**
```kotlin
id("conventions.testing-junit")
```

**If** the module has JUnit 4 Java tests (check for `@RunWith` or `import org.junit.Test` in
`.java` files), also add:
```kotlin
id("conventions.testing-junit-vintage")
```

**Special case — `driver-reactive-streams`**: This module uses `setSrcDirs` which replaces all
dirs. Ensure the source set includes all directories:
```kotlin
sourceSets {
    test {
        java {
            setSrcDirs(listOf("src/test/tck", "src/test/unit", "src/test/functional", "src/examples"))
        }
    }
}
```

### Step 2 — Migrate each `.groovy` spec to `.java`

For each Spock `.groovy` file in the module:

1. Read the Spock spec.
2. **Save a copy** for later logic-equivalence review:
   ```bash
   mkdir -p /tmp/spock-originals-<module>
   cp <path/to/Spec.groovy> /tmp/spock-originals-<module>/
   ```
3. Create a new `.java` file in the **same source set** (`src/test/unit` or `src/test/functional`).
4. Apply the translations from the **Pattern Reference** section below.
5. Preserve every test — use `@DisplayName` with the exact Spock test name string.
6. Delete the `.groovy` file.

Migrate unit tests before functional tests within each module.

### Step 3 — Compile

```bash
./gradlew :<module>:compileTestJava
```

Fix all errors. Check the **Known Issues** section below — most compilation failures match a
documented pattern.

### Step 4 — Run tests

```bash
./gradlew :<module>:cleanTest :<module>:test
```

All tests must pass (functional tests that need MongoDB will skip — that is OK).

### Step 5 — Verify (acceptance gate)

Run ALL of the following checks. Every check must pass before committing.

**Check 1 — Every Spock class has a JUnit replacement:**
```bash
# List new Java test classes
find <module>/src/test -name '*Test.java' -newer <module>/build.gradle.kts \
    -exec basename {} .java \; | sort > /tmp/junit-classes-<module>.txt

# Normalise Spock names: FooSpecification → FooTest
sed 's/Specification$/Test/' /tmp/spock-classes-<module>.txt > /tmp/spock-normalised-<module>.txt

# Find missing — output must be empty
comm -23 /tmp/spock-normalised-<module>.txt /tmp/junit-classes-<module>.txt
```
**FAIL condition**: Any class listed in the output → a spec was deleted without replacement.

**Check 2 — Every Spock test name is accounted for:**
```bash
# Extract JUnit 5 test names
find <module>/src/test -name '*Test.java' -exec \
    sed -n 's/.*@DisplayName("\(.*\)").*/\1/p' {} \; \
    | sort > /tmp/junit5-test-names-<module>.txt

# Find names in Spock that are missing from JUnit 5
comm -23 /tmp/spock-test-names-<module>.txt /tmp/junit5-test-names-<module>.txt \
    > /tmp/missing-tests-<module>.txt

cat /tmp/missing-tests-<module>.txt
```
**PASS conditions** (any missing name must match one of these):
- Name contains `#variable` (Spock interpolation) → verify the parameterized test exists with a simplified name.
- Test was intentionally merged into another test → note which test covers it.
- Test was split into multiple tests → verify all parts exist.

**FAIL condition**: A test name is missing with no explanation.

**Check 3 — No Groovy files remain:**
```bash
find <module>/src/test -name '*.groovy'
```
**FAIL condition**: Any output.

**Check 4 — No Spock imports in Java files:**
```bash
grep -r "import spock" <module>/src/test --include="*.java"
```
**FAIL condition**: Any output.

**Check 5 — Logic equivalence (sub-agent review):**

For each migrated Spock spec, spawn a sub-agent (using the `Agent` tool with `subagent_type=Review`)
to verify that the JUnit replacement preserves the original test logic. The sub-agent receives the
original `.groovy` source and the new `.java` source as context.

To avoid losing the original Spock source after deletion, **before deleting each `.groovy` file in
Step 2**, save a copy to `/tmp/spock-originals-<module>/`. Then use these copies during this check.

Procedure — run this for every migrated spec pair:

1. Use the `Agent` tool (with `model=claude-sonnet-4-6`) to spawn a review sub-agent with this prompt:

   ```
   You are reviewing a Spock-to-JUnit 5 test migration for logic equivalence.

   ## Original Spock spec
   <contents of /tmp/spock-originals-<module>/<SpecName>.groovy>

   ## Migrated JUnit 5 test
   <contents of <module>/src/test/.../<TestName>.java>

   Compare the two files and answer:
   1. Does every Spock test method (`def "..."`) have a corresponding JUnit test method?
      List each Spock test name and its JUnit counterpart.
   2. Is the assertion logic equivalent? Flag any test where:
      - An assertion was dropped or weakened.
      - A mock interaction verification (e.g., `1 * mock.method()`) was not translated to
        a Mockito `verify()` call.
      - A `where:` data table has fewer parameter combinations in the JUnit `@MethodSource`.
      - Exception testing (`thrown()`) was changed or removed.
      - Setup/teardown logic (`setup()`/`cleanup()`) was lost.
   3. Are there any behavioural differences introduced by the migration?
      (e.g., strict vs lenient mocking, execution order changes, missing cleanup)

   Output format:
   - PASS: All test logic is equivalent.
   - FAIL: <list each discrepancy with the Spock test name and description of what differs>
   ```

2. If any sub-agent returns **FAIL**, fix the discrepancies before proceeding.
3. Collect all sub-agent results. Report a summary to the user in the acceptance gate:
   ```
   Logic equivalence: <N>/<total> specs passed review
   Failures: <list any that failed and how they were fixed>
   ```

**FAIL condition**: Any spec pair where a sub-agent reports FAIL and the discrepancy has not been
resolved.

**Check 6 — Test count:**

Collect post-migration metrics using the same Python script from the baseline step.

| Metric | Acceptance Criterion |
|---|---|
| Test count | **MUST NOT** decrease vs baseline. If the post count is lower, investigate and fix before proceeding. Test count increases are expected (Spock `where:` = 1 test; JUnit `@ParameterizedTest` = N tests). |

**FAIL condition**: Post-migration test count < baseline test count.

**Check 7 — Test coverage:**

| Metric | Acceptance Criterion |
|---|---|
| Line coverage | **SHOULD NOT** decrease vs baseline. |

If line coverage **decreases**, Claude MUST:
1. Identify and explain **why** coverage dropped (e.g., a Spock test exercised code implicitly that
   the JUnit version does not, parameterised expansion changed coverage accounting, etc.).
2. Present the explanation to the user with the exact numbers:
   ```
   Coverage change for <module>: <before>% → <after>% (delta: <-N>%)
   Reason: <explanation>
   Accept this coverage change? (Y/N)
   ```
3. If the user answers **Y** — record the explanation in the Cross-Run Log and proceed.
4. If the user answers **N** — ask the user for input. The user may:
   - Clarify the actual reason for the drop.
   - Suggest a specific fix or additional test to recover coverage.
   Claude must then implement the user's suggestion, re-run tests, and re-check coverage.
   Repeat until the user accepts.

### Step 6 — User Acceptance Gate

Present a summary of all checks to the user:

```
Module: <module>
Spock specs migrated: <N>
Groovy files remaining: 0
Test count: <before> → <after> (delta: <+/-N>)
Line coverage: <before>% → <after>% (delta: <+/-N>%)
Missing test names: <count, with explanation if any>
Logic equivalence: <N>/<total> specs passed sub-agent review

All acceptance checks passed. Proceed to commit? (Y/N)
```

**Do NOT commit or move to the next module until the user explicitly approves.**

If the user answers **N**, ask what needs to change, implement the changes, re-run checks, and
present the summary again.

### Step 7 — Commit

```
Migrate <module> Spock specs to JUnit 5 (<N> files)
```

### Step 8 — Update the Cross-Run Log

Append findings to the **Cross-Run Log** section at the bottom of this file. Include:
- Any non-obvious fixes applied
- Any deviations from the standard procedure
- Any new patterns encountered not already documented
- Final test count and coverage numbers
- Any coverage explanations accepted by the user

---

## Post-Migration Cleanup (once, after all modules)

### Remove Spock/Groovy from build

**`gradle/libs.versions.toml`** — delete these entries:
- Versions: `groovy`, `spock-bom`
- Libraries: `spock-bom`, `spock-core`, `spock-junit4`, `groovy`
- Bundles: `spock`

**`buildSrc/src/main/kotlin/conventions/`** — delete:
- `testing-spock.gradle.kts`
- `testing-spock-exclude-slow.gradle.kts`

**`config/`** — delete:
- `config/codenarc/codenarc.xml`
- `config/spock/ExcludeSlow.groovy`
- `config/spock/OnlySlow.groovy`

### Check if JUnit Vintage is still needed

```bash
grep -r "import org.junit.Test" --include="*.java" -l
grep -r "@RunWith" --include="*.java" -l
```

If no matches, also remove `testing-junit-vintage.gradle.kts` and the `junit-vintage` bundle.

### Final verification

```bash
# 1. Clean build
./gradlew clean build --continue

# 2. No Groovy/Spock/CodeNarc references in build files
grep -ri "spock\|codenarc\|groovy" \
    --include="*.kts" --include="*.toml" --include="*.xml" --include="*.gradle"
# Must return zero results

# 3. No .groovy files in test directories
find . -path '*/src/test*' -name '*.groovy'
# Must return zero results
```

Commit:
```
Remove Spock/Groovy build infrastructure
```

---

## Pattern Reference

### Spock → JUnit 5 Translation Table

| Spock | JUnit 5 |
|---|---|
| `extends Specification` | Plain class, no base class |
| `def setup()` | `@BeforeEach void setUp()` |
| `def cleanup()` | `@AfterEach void tearDown()` |
| `def setupSpec()` | `@BeforeAll static void beforeAll()` |
| `def cleanupSpec()` | `@AfterAll static void afterAll()` |
| `def "test name"()` | `@Test @DisplayName("test name") void testName()` |
| `given:` / `when:` / `then:` | Arrange / Act / Assert inline |
| `expect:` | Single assertion block |
| `where:` data table | `@ParameterizedTest` + `@MethodSource` returning `Stream<Arguments>` |
| `thrown(XException)` | `assertThrows(XException.class, () -> ...)` |
| `notThrown(XException)` | Just call the code (no assertion needed) |
| `Mock()` / `Stub()` | `Mockito.mock()` or `@Mock` with `@ExtendWith(MockitoExtension.class)` |
| `_ >> value` | `when(mock.method()).thenReturn(value)` |
| `>>> [a, b, c]` | `when(mock.method()).thenReturn(a, b, c)` |
| `>> { closure }` | `doAnswer(invocation -> { ... }).when(mock).method(...)` |
| `1 * mock.method()` | `verify(mock, times(1)).method()` |
| `0 * mock.method()` | `verify(mock, never()).method()` |
| `*_` (any args) | One `any()` matcher per parameter in the method signature |
| `_ as Type` | `any(Type.class)` |
| `@Slow` | `@Tag("Slow")` |
| `@Unroll` | Implicit with `@ParameterizedTest` |
| `@Shared` | `static` field or `@BeforeAll` |
| `with(obj) { ... }` | Extract variable, assert each field with `assertEquals` |

### Groovy → Java Syntax

```
Groovy                              Java
──────────────────────────────────  ──────────────────────────────────────────
['a', 'b', 'c']                    Arrays.asList("a", "b", "c")
['key': value]                     new BasicBSONObject("key", value)
[1, 2, 3] as byte[]               new byte[]{1, 2, 3}
~['a': 1]                          new BasicBSONObject("a", 1)
"text ${var} more"                 "text " + var + " more"
obj.property                       obj.getProperty()
def x = expr                       explicit type or var
expr as Type                       (Type) expr
{-> body }                         () -> body
{ arg -> body }                    arg -> body
{ it -> body }                     arg -> body  (name the param)
x == y  (in then: block)           assertEquals(y, x)
x != null                          assertNotNull(x)
!x                                 assertFalse(x) or assertNull(x)
x instanceof Type                  assertInstanceOf(Type.class, x)
thrown(FooException)                assertThrows(FooException.class, () -> { ... })
notThrown(FooException)            (just call the code)
[a, b, c].collect { f(it) }       Stream.of(a, b, c).map(x -> f(x)).collect(toList())
```

### Challenging Pattern Examples

**Spock `where:` with complex types → `@MethodSource`:**
```groovy
// BEFORE
where:
representation | value                                         | document
JAVA_LEGACY    | [UUID.fromString('08070605-0403-0201-...')] | '{"array": [...]}'
STANDARD       | [new Binary((byte) 3, [1, 2, 3] as byte[])] | '{"array": [...]}'
```
```java
// AFTER
private static Stream<Arguments> args() {
    return Stream.of(
        Arguments.of(JAVA_LEGACY,
            singletonList(UUID.fromString("08070605-0403-0201-...")), "{\"array\": [...]}"),
        Arguments.of(STANDARD,
            singletonList(new Binary((byte) 3, new byte[]{1, 2, 3})), "{\"array\": [...]}")
    );
}
```

**Spock `Stub` with closure → Mockito:**
```groovy
// BEFORE
def cluster = Stub(Cluster) { getCurrentDescription() >> connectedDescription }
```
```java
// AFTER
Cluster cluster = mock(Cluster.class);
when(cluster.getCurrentDescription()).thenReturn(connectedDescription);
```

**Spock `>> { }` callback invocation → `doAnswer`:**
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

**Spock GString dynamic method invocation (no Java equivalent):**
```groovy
// BEFORE
1 * callback."$method"(* _) >> { assert it == args }
```
Migrate to output-based assertions: verify the decoded result instead of mock interactions.

**Groovy operator overloading:**
```groovy
// BEFORE
Map.metaClass.bitwiseNegate = { new BasicBSONObject(delegate as Map) }
document == ~['a': 1]
```
```java
// AFTER
assertEquals(new BasicBSONObject("a", 1), document);
```

### Style References

Look at these existing tests for idiomatic patterns:

| Style | File |
|---|---|
| Unit test with Mockito | `driver-core/.../ChangeStreamBatchCursorTest.java` |
| Functional test | `driver-sync/.../CrudProseTest.java` |
| Parameterized test | `bson/.../BsonBinaryReaderTest.java` |

---

## Known Issues

These were all encountered during a prior trial migration. When you hit a compilation or test
failure, check this list first.

### 1. Java static dispatch vs Groovy dynamic dispatch

**Symptom**: Test passes a `BasicDBObject` to a method parameter declared as `Object`, but the
wrong overload is called (e.g., `findOne(Object id)` instead of `findOne(DBObject query)`).

**Fix**: Add `instanceof` check with cast:
```java
if (criteria instanceof DBObject) {
    assertEquals(result, collection.findOne((DBObject) criteria));
} else {
    assertEquals(result, collection.findOne(criteria));
}
```

### 2. Java 8 release target — no Java 9+ APIs

**Symptom**: `cannot find symbol: method repeat(int)` or similar for `List.of()`, `Map.of()`.

**Cause**: Project compiles with `options.release.set(8)`. Groovyc ignored this.

**Fix**:
- `"a".repeat(n)` → `new String(new char[n]).replace('\0', 'a')` or `Arrays.fill` helper
- `List.of(a, b)` → `Arrays.asList(a, b)`
- `Map.of(k, v)` → `Collections.singletonMap(k, v)`

### 3. Mockito strict stubbing

**Symptom**: `UnnecessaryStubbingException` from `MockitoExtension`.

**Cause**: Spock is lenient by default; JUnit 5 MockitoExtension is strict.

**Fix**: `Mockito.lenient().when(...)` or `Mockito.lenient().doAnswer(...)` for shared stubs.

### 4. JaCoCo `$jacocoData` in reflection

**Symptom**: Assertion failure with `{$jacocoData=Optional[[Z@...]}` in the diff.

**Cause**: JaCoCo injects a synthetic field picked up by reflection-based comparisons.

**Fix**: Add `&& !field.isSynthetic()` to reflection field filters.

### 5. javac stricter than groovyc

**Symptom**: Type inference errors in pre-existing `.java` files (e.g., `Optional.flatMap()`).

**Cause**: The Spock convention compiled all test code with groovyc, including `.java` files.
After switching to JUnit, javac compiles them for the first time.

**Fix**: Rewrite generics-heavy code to use explicit `isPresent()` + `get()`.

### 6. Missing imports after convention switch

**Symptom**: `cannot find symbol` for classes that were previously available.

**Cause**: groovyc resolved imports that javac cannot.

**Fix**: Add explicit imports. Always run `compileTestJava` before `test`.

---

## Cross-Run Log

This section is updated by Claude after each module migration. Read it before starting a new
module. Append to it after completing each module.

### Format

```
### <module> — completed <date>
- Specs migrated: <N>
- Before: <test count> tests, <line%> line coverage
- After: <test count> tests, <line%> line coverage
- Coverage delta explanation: <if coverage dropped, the accepted explanation; "N/A" if no drop>
- Issues: <list any non-obvious problems and fixes>
- New patterns: <any translations not in the Pattern Reference>
- Deviations: <anything done differently from the standard procedure>
- User accepted: Yes
```

### driver-reactive-streams — completed (trial run)

- Specs migrated: 9
- Before: 654 tests, 72.9% line coverage
- After: 3,428 tests, 75.6% line coverage
- Issues:
  - `setSrcDirs()` replaces all dirs — must list all 4: tck, unit, functional, examples
  - Needs `testing-junit-vintage` for JUnit 4 tests (`@RunWith(Parameterized.class)`)
  - Pre-existing Java files failed under javac:
    - `TestHelper.java`: `Optional.flatMap()` type inference → rewrite to `isPresent()`/`get()`
    - `TestHelper.java`: JaCoCo `$jacocoData` field → add `!field.isSynthetic()` filter
    - `MongoClientSessionTest.java`: missing `import com.mongodb.MongoClientSettings`
  - `ClientSideEncryptionBsonSizeLimitsTest.java`, `GridFSPublisherTest.java`: used
    `String.repeat()` (Java 11+) → replaced with `Arrays.fill` helper
  - `GridFSPublisherTest.java`: `fileInfo.getId().getValue()` → `fileInfo.getObjectId()`
  - `ClientSessionBindingTest.java`: Mockito strict stubbing → `Mockito.lenient()`
- Deviations: None

### driver-legacy — completed (trial run)

- Specs migrated: 27
- Before: 687 tests, 86.7% line coverage
- After: 652 tests, 86.7% line coverage
- Issues:
  - `DBCollectionFunctionalTest.findOne()`: Groovy dynamically dispatches to
    `findOne(DBObject)` but Java statically dispatches to `findOne(Object id)`.
    Fix: `instanceof` check with explicit cast.
- New patterns: None
- Deviations: None

### driver-sync — completed (trial run)

- Specs migrated: 30
- Before: 1,763 tests, 74.7% line coverage
- After: 3,689 tests, 73.6% line coverage
- Issues: None beyond documented patterns
- New patterns: None
- Deviations: None

### bson — completed (trial run)

- Specs migrated: 59
- Before: 2,817 tests, 86.4% line coverage
- After: 3,721 tests, 87.4% line coverage
- Issues:
  - Heavy `~['key': val]` operator overloading → expand to `new BasicBSONObject("key", val)`
  - Heavy byte array data tables: `[1, 2, 3] as byte[]` → `new byte[]{1, 2, 3}`
  - `BasicBSONDecoderSpecification`: dynamic method invocation via GString
    (`callback."$method"(*_)`) → migrated to output-based assertions
- New patterns: None
- Deviations: None

### driver-core — completed (trial run)

- Specs migrated: 202
- Before: 4,885 tests, 69.5% line coverage
- After: 4,461 tests, 66.6% line coverage
- Issues:
  - 514 mock interaction verifications required careful translation
  - Complex nested `Stub` closures with `>>>` chained responses
  - `CommandBatchCursorSpecification` is the most complex spec — `Mock` with `>>` closures
    that invoke callbacks and assert counts mid-invocation
- New patterns: None
- Deviations: Migrated in two batches (unit tests first, then functional)

---

## Appendix: Prior Trial Migration Results

| Module | Before Tests | Before Line% | After Tests | After Line% | Delta Tests | Delta Line% |
|---|---:|---:|---:|---:|---:|---:|
| bson | 2,817 | 86.4% | 3,721 | 87.4% | +904 | +1.0% |
| driver-core | 4,885 | 69.5% | 4,461 | 66.6% | -424 | -2.9% |
| driver-sync | 1,763 | 74.7% | 3,689 | 73.6% | +1,926 | -1.1% |
| driver-legacy | 687 | 86.7% | 652 | 86.7% | -35 | +0.0% |
| driver-reactive-streams | 654 | 72.9% | 3,428 | 75.6% | +2,774 | +2.7% |
| **Total** | **10,806** | | **15,951** | | **+5,145** | |

Net reduction: 17,372 lines (52,120 added, 69,492 removed across 676 files).

Test count increases are a reporting artifact: Spock `where:` blocks count as 1 test; JUnit 5
`@ParameterizedTest` counts each parameter combination individually. Coverage stayed within 3%.
