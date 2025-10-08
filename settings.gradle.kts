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

pluginManagement {
    repositories {
        gradlePluginPortal()
        google()
        mavenCentral()
    }
}

include(":bom")

include(":bson")
include(":bson-record-codec")
include(":bson-kotlin")
include(":bson-kotlinx")
include(":bson-scala")

include(":driver-core")
include(":driver-sync")
include(":driver-legacy")
include(":driver-reactive-streams")
include(":mongodb-crypt")

include(":driver-kotlin-coroutine")
include(":driver-kotlin-extensions")
include(":driver-kotlin-sync")
include(":driver-scala")

include(":driver-benchmarks")
include(":driver-lambda")

// Set paths
project(":bom").projectDir = file("driver/bom")
project(":bson").projectDir = file("driver/bson/bson")
project(":bson-record-codec").projectDir = file("driver/bson/record-codec")
project(":bson-kotlin").projectDir = file("driver/kotlin/bson/kotlin")
project(":bson-kotlinx").projectDir = file("driver/kotlin/bson/kotlinx")
project(":bson-scala").projectDir = file("driver/scala/bson")

project(":driver-core").projectDir = file("driver/core")
project(":driver-sync").projectDir = file("driver/sync")
project(":driver-legacy").projectDir = file("driver/legacy")
project(":driver-reactive-streams").projectDir = file("driver/reactive-streams")
project(":mongodb-crypt").projectDir = file("driver/mongodb-crypt")

project(":driver-kotlin-coroutine").projectDir = file("driver/kotlin/coroutine")
project(":driver-kotlin-extensions").projectDir = file("driver/kotlin/extensions")
project(":driver-kotlin-sync").projectDir = file("driver/kotlin/sync")
project(":driver-scala").projectDir = file("driver/scala/reactive-streams")

project(":driver-benchmarks").projectDir = file("testing/benchmarks/")
project(":driver-lambda").projectDir = file("testing/lambda/")


if (providers.gradleProperty("includeGraalvm").isPresent) {
    include(":graalvm-native-image-app")
    project(":graalvm-native-image-app").projectDir = file("testing/lambda/graalvm-native-image-app")
}
