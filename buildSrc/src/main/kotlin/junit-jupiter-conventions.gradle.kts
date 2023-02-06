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

import org.gradle.accessors.dm.LibrariesForLibs
import org.gradle.kotlin.dsl.checkstyle
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.`java-library`
import org.gradle.kotlin.dsl.the

val libs = the<LibrariesForLibs>()

plugins {
    `java-library`
    id("com.adarshr.test-logger")
}


@Suppress("UnstableApiUsage")
dependencies {
    testImplementation(platform(libs.junitBom))
    testImplementation(libs.junitJupiter)
    testImplementation(libs.junitJupiterParams)
    testImplementation(libs.junitJupiterEngine)
    testImplementation(libs.junitVintageEngine)
}

tasks.withType<Test> {
    tasks.getByName("check").dependsOn(this)
    useJUnitPlatform {
        includeEngines("junit-jupiter")
    }
}





