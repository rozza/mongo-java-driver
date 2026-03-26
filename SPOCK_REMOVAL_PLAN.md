# Spock Removal Plan

## Overview

Remove all 323 Spock (Groovy) test specifications from the MongoDB Java Driver and replace them with
JUnit 5 tests. This eliminates the Groovy/Spock toolchain dependency and standardises on a single test
framework.

---

## Scope

| Module                  | Spock files | Convention plugin used                   |
|-------------------------|-------------|------------------------------------------|
| `bson`                  | 59          | `conventions.testing-spock`              |
| `driver-core`           | 199         | `conventions.testing-spock-exclude-slow` |
| `driver-sync`           | 29          | `conventions.testing-spock-exclude-slow` |
| `driver-legacy`         | 27          | `conventions.testing-spock-exclude-slow` |
| `driver-reactive-streams` | 9         | `conventions.testing-spock-exclude-slow` |
| **Total**               | **323**     |                                          |

---

## Phase 0 — Add JaCoCo Coverage Plugin

Add JaCoCo support so we can measure coverage before and after each migration and ensure no regression.

### 0.1 Create `conventions/testing-coverage.gradle.kts`

Create a new convention plugin in `buildSrc/src/main/kotlin/conventions/testing-coverage.gradle.kts`:

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
`buildSrc/src/main/kotlin/conventions/testing-base.gradle.kts`. This ensures JaCoCo is active for
**all** test configurations (JUnit, Spock, integration) automatically.

### 0.3 Verify baseline

Run `./gradlew test jacocoTestReport --continue` across all modules and record the per-module coverage baseline.
Store these values for comparison during migration.

---

## Phase 1 — Migrate Spock Tests to JUnit 5

Migrate one Spock specification at a time, following the process below. Reference implementations:
- **Unit test style**: `driver-core/.../ChangeStreamBatchCursorTest.java` (mocking with Mockito, `@BeforeEach`, `@DisplayName`)
- **Functional test style**: `driver-sync/.../CrudProseTest.java` (parameterised tests, `@ParameterizedTest`, `@MethodSource`)

### Per-file migration process

For **each** Spock specification:

1. **Record coverage** — Run the individual Spock test and capture its JaCoCo coverage value for the
   classes under test.

2. **Convert to JUnit 5** — Create a new Java test class following the mapping below. Place it in the
   same source set (`src/test/unit` or `src/test/functional`).

3. **Verify coverage** — Run the new JUnit 5 test and confirm its coverage **meets or exceeds** the
   value recorded in step 1.

4. **Delete the Spock file** — Remove the `.groovy` specification.

5. **Commit** — One commit per specification (or per logical group of related specs) to keep the
    history bisectable.

### Spock → JUnit 5 mapping reference

| Spock concept                        | JUnit 5 equivalent                                               |
|--------------------------------------|------------------------------------------------------------------|
| `extends Specification`              | Plain class, no base class                                       |
| `def setup()`                        | `@BeforeEach void setUp()`                                       |
| `def cleanup()`                      | `@AfterEach void tearDown()`                                     |
| `def setupSpec()`                    | `@BeforeAll static void beforeAll()`                             |
| `def cleanupSpec()`                  | `@AfterAll static void afterAll()`                               |
| `def "test name"()`                  | `@Test @DisplayName("test name") void testName()`                |
| `given: / when: / then:`            | Arrange / Act / Assert (inline comments if needed)               |
| `expect:`                            | Single assertion block                                           |
| `where:` (data table)               | `@ParameterizedTest` + `@MethodSource` or `@CsvSource`          |
| `thrown(ExceptionType)`              | `assertThrows(ExceptionType.class, () -> ...)`                   |
| `notThrown(ExceptionType)`           | `assertDoesNotThrow(() -> ...)`                                  |
| `interaction { }`                    | Mockito `verify()` / `when().thenReturn()`                       |
| `Mock()` / `Stub()`                 | `Mockito.mock()` or `@Mock` with `@ExtendWith(MockitoExtension)` |
| `_ >> value`                         | `when(mock.method()).thenReturn(value)`                           |
| `1 * mock.method()`                 | `verify(mock, times(1)).method()`                                |
| `@Slow` tag                         | `@Tag("Slow")`                                                   |
| `@Unroll`                            | Built-in with `@ParameterizedTest`                               |
| Groovy list/map literals            | `List.of()` / `Map.of()` or `Arrays.asList()`                   |
| Groovy power assertions             | AssertJ or JUnit `assertEquals` / `assertTrue`                   |

### Suggested migration order

Migrate modules from simplest to most complex to build momentum and refine the process:

1. **`driver-reactive-streams`** (9 files) — smallest module, quick wins
2. **`driver-legacy`** (27 files) — relatively small, self-contained
3. **`driver-sync`** (29 files) — moderate size
4. **`bson`** (59 files) — standalone, no driver dependencies
5. **`driver-core`** (199 files) — largest module, migrate last with the refined process

Within each module, migrate unit tests before functional tests (unit tests are simpler and less
dependent on infrastructure).

---

## Phase 2 — Remove Spock/Groovy from Dependencies

Once **all** Spock specifications have been migrated and deleted:

### 2.1 Remove from `gradle/libs.versions.toml`

Delete the following entries:

**Versions:**
```
groovy = "3.0.9"
spock-bom = "2.1-groovy-3.0"
```

**Libraries:**
```
spock-bom
spock-core
spock-junit4
groovy
```

**Bundles:**
```
spock = ["spock-core", "spock-junit4"]
junit-vintage = [...]   # remove if no other JUnit 4 tests remain
```

**Also review:**
- `junit-vintage-engine` — remove if no other JUnit 4 / Vintage tests remain after Spock removal.

### 2.2 Remove Spock convention references from module `build.gradle.kts` files

In each module's `build.gradle.kts`, remove:
```kotlin
id("conventions.testing-spock")
id("conventions.testing-spock-exclude-slow")
```

Affected files:
- `bson/build.gradle.kts`
- `driver-core/build.gradle.kts`
- `driver-sync/build.gradle.kts`
- `driver-legacy/build.gradle.kts`
- `driver-reactive-streams/build.gradle.kts`

### 2.3 Migrate Slow test handling

The Spock `@Slow` tag and `testSlowOnly` task are defined via `testing-spock-exclude-slow.gradle.kts`.
If slow-test tagging is still needed:
- Use JUnit 5 `@Tag("Slow")` on the migrated tests.
- Move the `excludeTags("Slow")` / `includeTags("Slow")` and `testSlowOnly` task registration into
  `testing-base.gradle.kts` or a new `testing-junit-exclude-slow.gradle.kts` convention.

---

## Phase 3 — Clean Up buildSrc

### 3.1 Delete Spock convention plugins

Remove these files from `buildSrc/src/main/kotlin/conventions/`:
- `testing-spock.gradle.kts`
- `testing-spock-exclude-slow.gradle.kts`

### 3.2 Delete CodeNarc convention plugin

Remove:
- `buildSrc/src/main/kotlin/conventions/codenarc.gradle.kts`

CodeNarc is only used by the Spock convention for Groovy quality checks. With no Groovy source files
remaining, it serves no purpose.

### 3.3 Delete CodeNarc and Spock config files

Remove:
- `config/codenarc/codenarc.xml`
- `config/spock/ExcludeSlow.groovy`
- `config/spock/OnlySlow.groovy`

### 3.4 Remove JUnit Vintage convention (if applicable)

If no other tests depend on the JUnit Vintage engine after Spock removal:
- Delete `buildSrc/src/main/kotlin/conventions/testing-junit-vintage.gradle.kts`
- Remove `junit-vintage` bundle and `junit-vintage-engine` library from `libs.versions.toml`

### 3.5 Verify clean build

Run:
```bash
./gradlew clean build --continue
```
Ensure no references to Groovy, Spock, or CodeNarc remain. Grep the entire project:
```bash
grep -ri "spock\|codenarc\|groovy" --include="*.kts" --include="*.toml" --include="*.xml" --include="*.gradle"
```

---

## Phase 4 — Final Validation

1. **Full test suite** — `./gradlew test  --continue` passes across all modules.
2. **Coverage comparison** — Compare final JaCoCo reports against Phase 0 baseline. Coverage must not
   decrease per module.
3. **CI pipeline** — Ensure CI (Evergreen) runs green with no Groovy/Spock references.
4. **Integration tests** — Run `./gradlew integrationTest --continue` if applicable to confirm no regressions.

---

## Additional Suggestions

### Consider using AI-assisted migration

Given the volume (323 files), use Claude Code to perform bulk conversions. The per-file process
(record coverage → convert → verify → delete) can be scripted with Claude performing the conversion
step and verifying coverage programmatically.

### Create a tracking ticket per module

Create JIRA sub-tasks under the parent Spock removal epic for each module. This allows parallel work
by multiple developers and clear progress tracking.

### Watch for Groovy-specific patterns

Some Spock tests may rely on Groovy language features that need careful translation:
- **Groovy closures** → Java lambdas (mind the `delegate` pattern — no direct Java equivalent)
- **Groovy truth** (empty collections, null, 0 are falsy) → explicit assertions
- **`with {}` blocks** → extract variable and assert individually
- **Property access syntax** (`obj.field` calling `getField()`) → explicit getter calls
- **String interpolation** (`"${var}"`) → `String.format()` or concatenation
- **Multi-assignment** (`def (a, b) = [1, 2]`) → separate variable declarations
- **Operator overloading** (`obj1 == obj2` calls `.equals()` in Groovy) → use `assertEquals`

### Remove the `groovy` plugin from buildSrc classpath

After all Groovy is removed, audit `buildSrc/build.gradle.kts` to confirm the Groovy Gradle plugin is
no longer on the classpath. If it was pulled in solely for Spock support, it can be removed.

### Consider adding JaCoCo coverage thresholds

Once migration is complete and coverage baselines are established, consider adding `jacocoTestCoverageVerification`
tasks with minimum coverage thresholds to prevent future regressions:

```kotlin
tasks.withType<JacocoCoverageVerification> {
    violationRules {
        rule {
            limit {
                minimum = "0.70".toBigDecimal() // adjust per module
            }
        }
    }
}
```

### Clean up source set configuration

The Spock convention disables Java source directories (`java { setSrcDirs(emptyList()) }`) because the
Groovy compiler handles mixed compilation. After removal, verify that `testing-junit.gradle.kts`
correctly configures `src/test`, `src/test/unit`, `src/test/functional` as Java source dirs — this is
already the case, so no change should be needed.
