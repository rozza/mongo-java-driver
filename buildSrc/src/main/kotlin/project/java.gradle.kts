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
    id("checkstyle")
    id("project.base")
    id("conventions.publishing")
    id("conventions.spotless")
    id("conventions.spotbugs")
    id("conventions.codenarc")
    id("conventions.javadoc")
    id("conventions.testing-junit")
}

dependencies {
    api(libs.slf4j) // TODO optional
}

val defaultJavaVersion = 17

logger.info("Compiling ${project.name} using JDK${defaultJavaVersion}")

java {
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8

    toolchain { languageVersion = JavaLanguageVersion.of(defaultJavaVersion) }

    withSourcesJar()
    withJavadocJar()
}

tasks.withType<JavaCompile> {
    options.encoding = "UTF-8"
    options.release.set(8)
}

sourceSets["main"].java { srcDir("src/main") }

val testJavaVersion: Int = findProperty("javaVersion")?.toString()?.toInt() ?: defaultJavaVersion

tasks.withType<Test> {
    exclude("tour/**")
    javaLauncher.set(javaToolchains.launcherFor { languageVersion = JavaLanguageVersion.of(testJavaVersion) })
}

tasks.findByName("docs")?.dependsOn(tasks.named("javadoc"))
