# Spock-to-JUnit 5 Migration

Instructions for Claude Code to migrate all Spock/Groovy test specifications to JUnit 5 Java,
removing the Groovy/Spock/CodeNarc toolchain dependency.

> **Convention**: HTML comments (`<!-- ... -->`) are human-readable notes explaining design
> decisions. Claude should skip them — they contain no instructions.

## Resume Protocol

This plan is designed to be stopped and restarted across multiple Claude sessions. All progress
is persisted to the `.migration/` directory in the project root.

**On every session start**, before doing any work:

1. Check if `.migration/progress.json` exists.
   - If it does **not** exist → this is a fresh start. Begin at **Pre-Migration Setup**.
   - If it **does** exist → read it and resume from wherever the plan left off.
2. Read `.migration/progress.json` to determine:
   - Whether pre-migration setup is complete.
   - Which module is currently in progress and at which step.
   - Which modules are already completed.
3. Skip all completed phases. Resume at the exact step indicated.
4. Read the **Cross-Run Log** section (bottom of this file) for context from prior sessions.

### Progress file: `.migration/progress.json`

```json
{
  "pre_migration_setup": "not-started | in-progress | completed",
  "baselines": {
    "<module>": {
      "test_count": 0,
      "line_pct": 0.0,
      "spock_spec_count": 0,
      "spock_test_count": 0
    }
  },
  "modules": {
    "driver-reactive-streams": { "status": "not-started | in-progress | completed", "step": 0 },
    "driver-legacy":           { "status": "not-started", "step": 0 },
    "driver-sync":             { "status": "not-started", "step": 0 },
    "bson":                    { "status": "not-started", "step": 0 },
    "driver-core":             { "status": "not-started", "step": 0 }
  },
  "post_migration_cleanup": "not-started | in-progress | completed"
}
```

Update `progress.json` at every step transition. This is the single source of truth for
where the plan is.

### Directory structure: `.migration/`

```
.migration/
├── progress.json              # current state (read on every session start)
├── baselines/
│   └── <module>.json          # baseline metrics per module
├── spock-originals/
│   └── <module>/              # copies of .groovy files before deletion
├── spock-names/
│   ├── <module>-test-names.txt    # extracted Spock test method names
│   └── <module>-class-names.txt   # extracted Spock class names
└── verification/
    ├── <module>-junit-classes.txt       # post-migration JUnit class names
    ├── <module>-junit-test-names.txt    # post-migration @DisplayName names
    └── <module>-missing-tests.txt       # any unaccounted test names
```

**Important**: Add `.migration/` to `.gitignore` during pre-migration setup so it is not committed.

---

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

**Resume guard**: Check `.migration/progress.json` — if `pre_migration_setup` is `"completed"`,
skip this entire section and proceed to **Per-Module Migration Procedure**.

### 0. Initialise progress tracking

```bash
mkdir -p .migration/baselines .migration/spock-originals .migration/spock-names .migration/verification
```

Create `.migration/progress.json` with all statuses set to `"not-started"` and all steps to `0`.

Add `.migration/` to `.gitignore`:
```bash
echo '.migration/' >> .gitignore
```

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

### 3. Collect baseline metrics and extract Spock test names

First, run all module tests to generate results and coverage reports:

```bash
./gradlew cleanTest test --continue
```

Then use sub-agents (via the `Agent` tool with `model=claude-haiku-4-5-20251001`) to collect
baselines for all 5 modules **in parallel**.

<!-- Haiku 4.5 rationale: each sub-agent performs mechanical tasks — running shell commands,
parsing XML files, extracting numbers/strings. No code reasoning is needed, so the cheapest
and fastest model is appropriate. -->

Spawn one sub-agent per module with this prompt:

```
Collect pre-migration baseline metrics for the module: <module>

## Part 1 — Test count and coverage

Parse the JUnit XML test results and JaCoCo coverage report:

1. Test count: count all <testcase> elements across XML files in
   `<module>/build/test-results/test/*.xml`. Sum the `tests` attribute from each root element.

2. Test names: extract every `classname::name` pair from <testcase> elements.

3. Line coverage: parse `<module>/build/reports/jacoco/test/jacocoTestReport.xml`.
   Find the <counter type="LINE"> element, compute: covered / (covered + missed) * 100.

## Part 2 — Spock test names and class names

Extract from source (before any migration changes):

1. Spock test names: find all `.groovy` files under `<module>/src/test` and extract test
   method names from lines matching `def 'name'` or `def "name"`.
   Save the sorted list to `.migration/spock-names/<module>-test-names.txt`.

2. Spock class names: find all `*Specification.groovy` files under `<module>/src/test` and
   extract the basename (without `.groovy`).
   Save the sorted list to `.migration/spock-names/<module>-class-names.txt`.

## Output
Save baseline to `.migration/baselines/<module>.json`:
```json
{ "test_count": <N>, "line_pct": <N>, "spock_spec_count": <N>, "spock_test_count": <N> }
```
Return a summary:
- Module: <module>
- Test count: <N>
- Line coverage: <N>%
- Spock specs found: <N>
- Spock test methods found: <N>
```

Expected baseline values from prior run (use to sanity-check results):

| Module | Tests | Line% |
|---|---:|---:|
| driver-reactive-streams | 654 | 72.9% |
| driver-legacy | 687 | 86.7% |
| driver-sync | 1,763 | 74.7% |
| bson | 2,817 | 86.4% |
| driver-core | 4,885 | 69.5% |

### 4. Commit the JaCoCo setup

```
Add JaCoCo coverage convention for Spock migration baseline
```

### 5. Mark pre-migration setup complete

Update `.migration/progress.json`: set `pre_migration_setup` to `"completed"` and populate
the `baselines` object with values from each module's `.migration/baselines/<module>.json`.

---

## Per-Module Migration Procedure

Repeat this procedure for each module in order. Do NOT skip steps.
Do NOT begin the next module until the current module has passed the User Acceptance Gate (Step 6).

**Resume guard**: Read `.migration/progress.json` to find the current module and step.
- Skip any module where `status` is `"completed"`.
- For a module where `status` is `"in-progress"`, resume at the recorded `step`.
- For the next `"not-started"` module, set it to `"in-progress"` with `step: 1` and begin.

**At the start of each step**, update `progress.json` with the current step number. This ensures
that if the session is interrupted mid-step, the next session knows exactly where to resume.

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

Before starting, save all Spock originals for later logic-equivalence review:
```bash
cp <module>/src/test/**/*.groovy .migration/spock-originals/<module>/
```

**Resume note**: If resuming mid-step-2, check which `.groovy` files have already been deleted
(and have a corresponding `.java` file). Only migrate the remaining `.groovy` files.

Migrate unit tests before functional tests within each module.

Use sub-agents (via the `Agent` tool with `model=claude-opus-4-6`) to translate each Spock spec
to Java.

<!-- Opus 4.6 rationale: spec complexity varies widely — simple specs are straightforward, but
complex ones involve nested mock closures with callback invocation (>> {}), chained responses
(>>>), mid-assertion mock interaction counts, GString dynamic method dispatch, and Groovy operator
overloading. Getting the translation wrong means debugging compilation failures, chasing subtle
test logic differences, and potentially missing coverage. Opus produces more reliable first-pass
translations, which reduces the fix-up cycle and avoids repeated logic-equivalence failures. The
higher per-call cost is offset by fewer iterations through Steps 3-5. Sonnet is not reliable
enough for the hardest specs (e.g., CommandBatchCursorSpecification with 514 mock interactions).
Haiku is too lightweight for any Groovy→Java translation involving mocks or DSL idioms. -->

For each Spock `.groovy` file, spawn a sub-agent with this prompt:

```
You are migrating a Spock/Groovy test specification to JUnit 5 Java.

## Source Spock spec
<contents of the .groovy file>

## Target location
<same source set path, e.g., src/test/unit/com/mongodb/.../>

## Translation rules
<include the Pattern Reference and Groovy → Java Syntax sections from this document>

## Known issues to watch for
<include the Known Issues section from this document>

## Instructions
1. Create a complete Java test class that is equivalent to the Spock spec.
2. Use `@DisplayName` with the **exact** Spock test name string for every test method.
3. For `where:` data tables, use `@ParameterizedTest` + `@MethodSource` — preserve every
   row from the data table.
4. Translate all mock interactions to Mockito equivalents.
5. Use Java 8 compatible APIs only (no `List.of()`, `Map.of()`, `String.repeat()`, etc.).
6. Do NOT add extra tests, helper methods, or refactoring beyond what the Spock spec contains.

## Output
Return ONLY the complete Java file content, ready to write. No explanation needed.
```

After each sub-agent returns:
1. Write the `.java` file to the correct location.
2. Delete the `.groovy` file.

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
    -exec basename {} .java \; | sort > .migration/verification/<module>-junit-classes.txt

# Normalise Spock names: FooSpecification → FooTest
sed 's/Specification$/Test/' .migration/spock-names/<module>-class-names.txt \
    > .migration/verification/<module>-spock-normalised.txt

# Find missing — output must be empty
comm -23 .migration/verification/<module>-spock-normalised.txt \
         .migration/verification/<module>-junit-classes.txt
```
**FAIL condition**: Any class listed in the output → a spec was deleted without replacement.

**Check 2 — Every Spock test name is accounted for:**
```bash
# Extract JUnit 5 test names
find <module>/src/test -name '*Test.java' -exec \
    sed -n 's/.*@DisplayName("\(.*\)").*/\1/p' {} \; \
    | sort > .migration/verification/<module>-junit-test-names.txt

# Find names in Spock that are missing from JUnit 5
comm -23 .migration/spock-names/<module>-test-names.txt \
         .migration/verification/<module>-junit-test-names.txt \
    > .migration/verification/<module>-missing-tests.txt

cat .migration/verification/<module>-missing-tests.txt
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

The original Spock sources were saved to `.migration/spock-originals/<module>/` during Step 2.

Procedure — run this for every migrated spec pair:

1. Use the `Agent` tool (with `model=claude-sonnet-4-6`) to spawn a review sub-agent.

   <!-- Sonnet 4.6 rationale: this is structured code comparison with a clear rubric — it doesn't
   require Opus-level reasoning, and it runs significantly faster when spawning one sub-agent per
   spec (up to 202 for driver-core). Haiku is too lightweight to reliably catch subtle differences
   in Groovy↔Java translation. -->

   Prompt:

   ```
   You are reviewing a Spock-to-JUnit 5 test migration for logic equivalence.

   ## Original Spock spec
   <contents of .migration/spock-originals/<module>/<SpecName>.groovy>

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

### Step 8 — Update progress and Cross-Run Log

1. Update `.migration/progress.json`: set the current module's `status` to `"completed"` and
   `step` to `8`.
2. Append findings to the **Cross-Run Log** section at the bottom of this file. Include:
   - Any non-obvious fixes applied
   - Any deviations from the standard procedure
   - Any new patterns encountered not already documented
   - Final test count and coverage numbers
   - Any coverage explanations accepted by the user
3. Commit the Cross-Run Log update to preserve it for the next session:
   ```
   Update SPOCK_MIGRATION.md cross-run log for <module>
   ```

---

## Post-Migration Cleanup (once, after all modules)

**Resume guard**: Only begin this section when ALL modules in `progress.json` have
`status: "completed"`. If `post_migration_cleanup` is already `"completed"`, skip this section.

Set `post_migration_cleanup` to `"in-progress"` in `progress.json` before starting.

### Remove Spock/Groovy from build, verify, and clean up

Use a sub-agent (via the `Agent` tool with `model=claude-sonnet-4-6`) to perform the full
post-migration cleanup: remove build infrastructure, check for leftover artifacts, and report.

<!-- Sonnet 4.6 rationale: this task combines file deletion, build file editing (removing entries
from libs.versions.toml and deleting convention plugins), and a codebase-wide artifact scan. It
requires reading and modifying structured files (TOML, Gradle KTS) which needs more capability
than Haiku's pattern matching, but doesn't need Opus-level reasoning since all the targets are
explicitly listed. Sonnet strikes the right balance of reliability and speed for a one-shot
cleanup task. -->

Spawn the sub-agent with this prompt:

```
You are performing post-migration cleanup for a Spock-to-JUnit 5 migration.
Execute ALL of the following steps in order.

## Step 1 — Remove Spock/Groovy dependency declarations

Edit `gradle/libs.versions.toml`:
- Delete version entries: `groovy`, `spock-bom`
- Delete library entries: `spock-bom`, `spock-core`, `spock-junit4`, `groovy`
- Delete bundle entries: `spock`

## Step 2 — Delete Spock convention plugins

Delete these files:
- `buildSrc/src/main/kotlin/conventions/testing-spock.gradle.kts`
- `buildSrc/src/main/kotlin/conventions/testing-spock-exclude-slow.gradle.kts`

## Step 3 — Delete Spock/CodeNarc config files

Delete these files/directories:
- `config/codenarc/codenarc.xml`
- `config/spock/ExcludeSlow.groovy`
- `config/spock/OnlySlow.groovy`
- Remove `config/codenarc/` and `config/spock/` directories if empty after deletion.

## Step 4 — Check if JUnit Vintage is still needed

Search for JUnit 4 usage:
- `import org.junit.Test` in any `.java` file
- `@RunWith` in any `.java` file

If NO matches are found, also delete:
- `buildSrc/src/main/kotlin/conventions/testing-junit-vintage.gradle.kts`
- The `junit-vintage` bundle from `gradle/libs.versions.toml`

If matches ARE found, list the files that still use JUnit 4.

## Step 5 — Scan for any remaining artifacts

Check ALL of the following:

1. `.groovy` files under any `*/src/test*` directory.
2. "spock" (case-insensitive) in all `.kts`, `.toml`, `.xml`, and `.gradle` files.
3. "groovy" (case-insensitive) in all `.kts`, `.toml`, `.xml`, and `.gradle` files.
   Exclude false positives in comments or documentation.
4. "codenarc" (case-insensitive) in all build and config files.
5. `config/codenarc/` or `config/spock/` directories still existing.
6. "import spock" in any `.java` file.
7. `testing-spock.gradle.kts` or `testing-spock-exclude-slow.gradle.kts` in
   `buildSrc/src/main/kotlin/conventions/`.

## Output

Report:
- Files deleted: <list>
- Files edited: <list with summary of changes>
- JUnit Vintage: <still needed (list files) | removed>
- Artifact scan: PASS | FAIL <list each finding with file path and line content>
```

After the sub-agent completes:

1. If the artifact scan reports **FAIL**, fix any remaining items before proceeding.
2. Run a clean build to verify everything compiles:
   ```bash
   ./gradlew clean build --continue
   ```
3. If the build fails, fix and re-run until it passes.

Commit:
```
Remove Spock/Groovy build infrastructure
```

### Clean up migration state

1. Update `.migration/progress.json`: set `post_migration_cleanup` to `"completed"`.
2. Optionally remove the `.migration/` directory (it has served its purpose).
   Ask the user before deleting — they may want to keep it for reference.
3. Remove `.migration/` from `.gitignore` if the directory is deleted.

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
