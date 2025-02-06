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
@Suppress("DSL_SCOPE_VIOLATION") // TODO: Remove once KTIJ-19369 is fixed
plugins {
    id("java-library")
    `kotlin-dsl`
    alias(libs.plugins.spotless)
    alias(libs.plugins.detekt) apply false
}

repositories {
    gradlePluginPortal()
    mavenCentral()
    google()
}

dependencies {
    // TODO not needed in Gradle 8.12? https://github.com/gradle/gradle/issues/15383
    implementation(files(libs.javaClass.superclass.protectionDomain.codeSource.location))

    // https://docs.gradle.org/current/userguide/implementing_gradle_plugins_precompiled.html#sec:applying_external_plugins
    implementation(libs.buildsrc.plugin.spotless)
    implementation(libs.buildsrc.plugin.spotbugs)
    implementation(libs.buildsrc.plugin.detekt)
    implementation(libs.buildsrc.plugin.test.logger)
}

spotless {
    kotlinGradle {
        ktfmt("0.39").dropboxStyle().configure { it.setMaxWidth(120) }
        trimTrailingWhitespace()
        indentWithSpaces()
        endWithNewline()
        licenseHeaderFile("../config/mongodb.license", "(group|plugins|import|buildscript|rootProject|@Suppress)")
    }
}

java { toolchain { languageVersion.set(JavaLanguageVersion.of("17")) } }

tasks.withType<GradleBuild> { dependsOn("spotlessApply") }
