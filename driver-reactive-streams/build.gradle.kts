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
import config.Extensions.setAll

plugins {
    id("project.java")
    id("conventions.test-artifacts")
    id("conventions.testing-mockito")
    id("conventions.testing-spock")
}

base.archivesName.set("mongodb-driver-reactivestreams")

extra.setAll(
    mapOf(
        "mavenName" to "The MongoDB Reactive Streams Driver",
        "mavenDescription" to "A Reactive Streams implementation of the MongoDB Java driver",
        "mavenArtifactId" to base.archivesName.get(),
        "automaticModuleName" to "org.mongodb.driver.reactivestreams",
        "bundleSymbolicName" to "org.mongodb.driver-reactivestreams",
        "importPackage" to
            listOf(
                    "com.mongodb.crypt.capi.*;resolution:=optional",
                    "com.mongodb.internal.crypt.capi.*;resolution:=optional",
                    "*" // import all that is not excluded or modified before
                    )
                .joinToString(",")))

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))
    api(libs.reactive.streams)
    implementation(platform(libs.project.reactor.bom))
    implementation(libs.project.reactor.core)

    testImplementation(libs.project.reactor.test)
    testImplementation(libs.reactive.streams.tck)
    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-sync", configuration = "default"))
    testImplementation(project(path = ":driver-sync", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "testArtifacts"))
    testImplementation(project(path = ":util:spock", configuration = "default"))
}

sourceSets { test { java { setSrcDirs(listOf("src/test/tck")) } } }

// Reactive Streams TCK uses TestNG
tasks.register("tckTest", Test::class) {
    useTestNG()
    maxParallelForks = 1
    isScanForTestClasses = false

    binaryResultsDirectory.set(layout.buildDirectory.dir("$name-results/binary"))
    reports.html.outputLocation.set(layout.buildDirectory.dir("reports/$name"))
    reports.junitXml.outputLocation.set(layout.buildDirectory.dir("reports/$name-results"))
}
