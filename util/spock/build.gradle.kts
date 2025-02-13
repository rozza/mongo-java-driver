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

plugins { id("project.java") }

extra.setAll(mapOf("automaticModuleName" to "N/A", "importPackage" to "N/A"))

tasks.withType<PublishToMavenRepository>().configureEach { enabled = false }

tasks.withType<PublishToMavenLocal>().configureEach { enabled = false }
