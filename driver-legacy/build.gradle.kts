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
    id("conventions.testing-junit-vintage")
    id("conventions.testing-spock")
    id("conventions.test-artifacts")
}

base.archivesName.set("mongodb-driver-legacy")

extra.setAll(
    mapOf(
        "mavenName" to "The Legacy MongoDB Driver",
        "mavenDescription" to "The Legacy MongoDB Driver",
        "mavenArtifactId" to base.archivesName.get(),
        "importPackage" to "org.slf4j.*;resolution:=optional", // TODO check this
        "automaticModuleName" to "org.mongodb"))

dependencies {
    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-core", configuration = "default"))
    api(project(path = ":driver-sync", configuration = "default"))

    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-sync", configuration = "testArtifacts"))
    testImplementation(project(path = ":util:spock", configuration = "default"))
}
