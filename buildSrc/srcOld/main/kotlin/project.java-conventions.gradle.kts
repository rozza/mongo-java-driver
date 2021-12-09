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

plugins {
    `java-library`
}

dependencies {
    compileOnly(libs("findbugsJsr305"))
    api(libs("slf4j"))
    testImplementation(libs("findbugsJsr305"))

    // https://issues.apache.org/jira/browse/GROOVY-10194
    testImplementation("org.codehaus.groovy:groovy-all:3.0.9")
}

fun libs(lib: String) =
    rootProject.extensions.getByType(VersionCatalogsExtension::class).named("libs").findDependency(lib).get()


/* Compiling */
//tasks.withType(AbstractCompile) {
//    options.encoding = "ISO-8859-1"
//    options.fork = true
//    options.debug = true
//    options.compilerArgs = ["-Xlint:all"]
//}
/*

sourceSets {
    main {
        java.srcDirs = ["src/main"]
    }
}

tasks.withType(GenerateModuleMetadata) {
    enabled = false
}
*/


