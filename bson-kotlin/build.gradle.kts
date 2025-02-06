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

plugins { id("project.kotin") }

base.archivesName.set("bson-kotlin")

extra.setAll(
    mapOf(
        "mavenName" to "Bson Kotlin",
        "mavenDescription" to "Bson Kotlin Codecs",
        "mavenUrl" to "https://bsonspec.org",
        "automaticModuleName" to "org.mongodb.bson.kotlin",
        "importPackage" to "org.slf4j.*;resolution:=optional",
        "mavenArtifactId" to base.archivesName.get()
    ))

import io.gitlab.arturbosch.detekt.Detekt
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    id("org.jetbrains.kotlin.jvm")
    id("java-library")
    id("com.mongodb.doclet")

    // Test based plugins
    alias(libs.plugins.spotless)
    alias(libs.plugins.dokka)
    alias(libs.plugins.detekt)
}

repositories {
    mavenCentral()
    google()
}

base.archivesName.set("bson-kotlin")

description = "Bson Kotlin Codecs"

ext.set("pomName", "Bson Kotlin")

dependencies {
    // Align versions of all Kotlin components
    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib.jdk8)

    api(project(path = ":bson", configuration = "default"))
    implementation(libs.kotlin.reflect)

    testImplementation(libs.junit.kotlin)
    testImplementation(project(path = ":driver-core", configuration = "default"))
}

kotlin { explicitApi() }

tasks.withType<KotlinCompile> { kotlinOptions.jvmTarget = "1.8" }



// TODO
// spotbugs { showProgress.set(true) }

// ===========================
//     Test Configuration
// ===========================

tasks.test { useJUnitPlatform() }

// ===========================
//     Dokka Configuration
// ===========================
val dokkaOutputDir = "${rootProject.buildDir}/docs/${base.archivesName.get()}"

tasks.dokkaHtml.configure {
    outputDirectory.set(file(dokkaOutputDir))
    moduleName.set(base.archivesName.get())
}

val cleanDokka by tasks.register<Delete>("cleanDokka") { delete(dokkaOutputDir) }

project.parent?.tasks?.named("docs") {
    dependsOn(tasks.dokkaHtml)
    mustRunAfter(cleanDokka)
}

tasks.javadocJar.configure {
    dependsOn(cleanDokka, tasks.dokkaHtml)
    archiveClassifier.set("javadoc")
    from(dokkaOutputDir)
}

// ===========================
//     Sources publishing configuration
// ===========================
tasks.sourcesJar { from(project.sourceSets.main.map { it.kotlin }) }

afterEvaluate { tasks.jar { manifest { attributes["Automatic-Module-Name"] = "org.mongodb.bson.kotlin" } } }
