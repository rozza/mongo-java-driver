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
package project

plugins {
    id("java-library")
    id("project.base")
    id("conventions.publishing")
    id("conventions.spotless")
    id("conventions.spotbugs")
    id("conventions.codenarc")
    id("conventions.javadoc")
    id("conventions.testing-junit")
}

/* Compiling */
dependencies {
    api(libs.slf4j) // TODO optional
}

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

sourceSets["main"].java { srcDir("src/main") }
sourceSets["integrationTest"].java.srcDir("src/integrationTest/java")

tasks.withType<Test> {
    exclude("tour/**")
}
