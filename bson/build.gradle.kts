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

// https://github.com/gradle/gradle/issues/22797
// @Suppress("UnstableApiUsage", "DSL_SCOPE_VIOLATION")
plugins {
    id("core-conventions")
    id("java-library-conventions")
    id("junit-jupiter-conventions")
}


dependencies {
    api(libs.slf4jApi) // TODO make optional
}


//jar {
//    baseName = "bson"
//}
description = "The BSON library"





//archivesBaseName = "bson"


description = "The BSON library"


//
//ext {
//    pomName = "BSON"
//    pomURL = "https://bsonspec.org"
//}
//
//jar.manifest.attributes["Import-Package"] = "org.slf4j.*;resolution:=optional"
