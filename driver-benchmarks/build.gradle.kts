/*
 * Copyright 2016-present MongoDB, Inc.
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

plugins {
    id("application")
    id("java-library")
}

application {
    mainClass = "com.mongodb.benchmark.benchmarks.BenchmarkSuite"
    applicationDefaultJvmArgs = listOf(
        "-Dorg.mongodb.benchmarks.data=${System.getProperty("org.mongodb.benchmarks.data")}",
        "-Dorg.mongodb.benchmarks.output=${System.getProperty("org.mongodb.benchmarks.output")}")
}

sourceSets {
    main {
        java { setSrcDirs(listOf("src/main")) }
        resources { setSrcDirs(listOf("src/resources")) }
    }
}

dependencies {
    // api(project(":driver-sync")) TODO Post sync
    api(project(":mongodb-crypt"))
    implementation(libs.logback.classic)
}

tasks.withType<Javadoc>().configureEach {
    enabled = false
}
