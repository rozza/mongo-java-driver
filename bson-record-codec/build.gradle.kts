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
import ProjectExtensions.configureJarManifest
import ProjectExtensions.configureMavenPublication

plugins {
    id("project.java")
    id("conventions.test-artifacts")
}

base.archivesName.set("bson-record-codec")

dependencies {
    api(project(path = ":bson", configuration = "default"))
    testImplementation(project(path = ":bson", configuration = "testArtifacts"))
    testImplementation(project(path = ":driver-core", configuration = "default"))
}

configureMavenPublication {
    pom {
        name.set("BSON Record Codec")
        description.set("The BSON Codec for Java records")
        url.set("https://bsonspec.org")
    }
}

configureJarManifest {
    attributes["Automatic-Module-Name"] = "org.mongodb.bson.record.codec"
    attributes["Bundle-SymbolicName"] = "org.mongodb.bson.record.codec"
}

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

tasks.withType<JavaCompile> { options.release.set(17) }

tasks.withType<Test>().configureEach { onlyIf { javaVersion.isCompatibleWith(JavaVersion.VERSION_17) } }
