/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package conventions


plugins {
    id("java-library")
}

// TODO: Migrate to using https://docs.gradle.org/current/userguide/java_testing.html#sec:java_test_fixtures
val testArtifacts by configurations.creating
val testArtifactJar by tasks.registering(Jar::class) {
    archiveBaseName.set("${project.name}-test")
    from(sourceSets.test.get().output)
}

// Silly but allows higher throughput of the build because we can start compiling / testing other modules while the tests run
// This works because the sourceSet 'integrationTest' extends 'test', so it won't compile until after 'test' is compiled, but the
// task graph goes 'compileTest*' -> 'test' -> 'compileIntegrationTest*' -> 'testJar'.
// By flipping the order of the graph slightly, we can unblock downstream consumers of the testJar to start running tasks while this project
// can be executing the 'test' task.
tasks.test {
    mustRunAfter(testArtifactJar)
}

artifacts {
    add("testArtifacts", testArtifactJar)
}
