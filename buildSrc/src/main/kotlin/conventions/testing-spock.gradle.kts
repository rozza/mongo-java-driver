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
package conventions

import org.gradle.api.tasks.testing.Test
import org.gradle.kotlin.dsl.dependencies
import org.gradle.kotlin.dsl.withType
import project.libs

plugins {
    id("conventions.testing-base")
    id("groovy")
}

dependencies {
    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.bundles.junit.vintage)

    testImplementation(platform(libs.spock.bom))
    testImplementation(libs.bundles.spock)
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform { includeEngines("spock") }
}

sourceSets {
    test {
        groovy {
            srcDirs("src/test", "src/test/unit", "src/test/functional")
        }

        // Disable java src directories as groovy allows mixed java and groovy code
        java {
            setSrcDirs(emptyList<Any>())
        }
    }
}

