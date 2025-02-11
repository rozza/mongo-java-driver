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
    id("project.kotlin")
    alias(libs.plugins.kotlin.serialization)
}

base.archivesName.set("bson-kotlinx")

extra.setAll(
    mapOf(
        "mavenName" to "Bson Kotlinx",
        "mavenDescription" to "Bson Kotlinx Codecs",
        "mavenUrl" to "https://bsonspec.org",
        "automaticModuleName" to "org.mongodb.bson.kotlinx",
        "importPackage" to "org.slf4j.*;resolution:=optional",
        "mavenArtifactId" to base.archivesName.get()))

java {
    registerFeature("dateTimeSupport") { usingSourceSet(sourceSets["main"]) }
    registerFeature("jsonSupport") { usingSourceSet(sourceSets["main"]) }
}

dependencies {
    implementation(platform(libs.kotlinx.serialization))
    implementation(libs.kotlinx.serialization.core)
    "dateTimeSupportImplementation"(libs.kotlinx.serialization.datetime) // TODO - is this optional in the wild?
    "jsonSupportImplementation"(libs.kotlinx.serialization.json)

    api(project(path = ":bson", configuration = "default"))
    implementation(libs.kotlin.reflect)

    testImplementation(project(path = ":driver-core", configuration = "default"))
    testImplementation(libs.kotlinx.serialization.datetime)
    testImplementation(libs.kotlinx.serialization.json)
}
