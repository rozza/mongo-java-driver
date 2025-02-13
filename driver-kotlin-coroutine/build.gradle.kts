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

plugins { id("project.kotlin") }

base.archivesName.set("mongodb-driver-kotlin-coroutine")

extra.setAll(
    mapOf(
        "mavenName" to "MongoDB Kotlin Coroutine Driver",
        "mavenDescription" to "The MongoDB Kotlin Coroutine Driver",
        "automaticModuleName" to "org.mongodb.driver.kotlin.coroutine",
        "importPackage" to "org.slf4j.*;resolution:=optional",
        "mavenArtifactId" to base.archivesName.get()))

dependencies {
    api(project(path = ":bson", configuration = "default"))
    implementation(libs.kotlin.reflect)

    testImplementation(project(path = ":driver-core", configuration = "default"))
}

dependencies {
    // Align versions of all Kotlin components
    implementation(platform(libs.kotlin.bom))
    implementation(libs.kotlin.stdlib.jdk8)

    implementation(platform(libs.kotlinx.coroutines.bom))
    api(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.reactive)

    api(project(path = ":bson", configuration = "default"))
    api(project(path = ":driver-reactive-streams", configuration = "default"))
    implementation(project(path = ":bson-kotlin", configuration = "default"))

    testImplementation(libs.kotlin.reflect)
    testImplementation(libs.junit.kotlin)
    testImplementation(libs.bundles.mockito.kotlin)
    testImplementation(libs.assertj)
    testImplementation(libs.classgraph)

    integrationTestImplementation(libs.junit.kotlin)
    integrationTestImplementation(libs.kotlinx.coroutines.test)
    integrationTestImplementation(project(path = ":driver-sync"))
    integrationTestImplementation(project(path = ":driver-core"))
    integrationTestImplementation(project(path = ":bson"))
}


