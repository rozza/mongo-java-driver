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
    id("project.java-legacy")
    id("conventions.test-artifacts")
}

base.archivesName.set("bson-record-codec")

extra.setAll(
    mapOf(
        "mavenName" to "BSON Record Codec",
        "mavenDescription" to "The BSON Codec for Java records'
        "automaticModuleName" to "org.mongodb.bson.record.codec",
        "bundleSymbolicName" to "org.mongodb.bson.record.codec",
        "importPackage" to "org.slf4j.*;resolution:=optional",
        "mavenArtifactId" to base.archivesName.get()))

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.release.set(17)
}

tasks.withType<Test>().configureEach {
    onlyIf { javaVersion.isCompatibleWith(javaVersion.VERSION_17) }
}

tasks.withType<Javadoc>().configureEach {
    dependsOn(project(':bson').tasks.withType<Javadoc>, project(':driver-core').tasks.withType<Javadoc>)
}
