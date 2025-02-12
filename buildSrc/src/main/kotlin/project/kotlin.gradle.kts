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

import org.gradle.jvm.toolchain.JavaLanguageVersion

plugins {
    id("java-library") // TODO NEEDED?
    kotlin("jvm")
    id("project.base")
    id("conventions.publishing")
    id("conventions.spotless")
    id("conventions.detekt")
    id("conventions.spotbugs")
    id("conventions.testing-junit")
}

/* Compiling */
kotlin { explicitApi() }

java { toolchain { languageVersion.set(JavaLanguageVersion.of("8")) } }

dependencies {
    // Align versions of all Kotlin components
    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib.jdk8)

    testImplementation(libs.junit.kotlin)
}

tasks.test { useJUnitPlatform() }
