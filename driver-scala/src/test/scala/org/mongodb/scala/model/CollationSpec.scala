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

package org.mongodb.scala.model

import java.lang.reflect.Modifier.isStatic

import com.mongodb.client.model.{
  Collation => JCollation,
  CollationAlternate => JCollationAlternate,
  CollationCaseFirst => JCollationCaseFirst,
  CollationMaxVariable => JCollationMaxVariable,
  CollationStrength => JCollationStrength
}
import org.mongodb.scala.BaseSpec

class CollationSpec extends BaseSpec {

  "Collation" should "just have a builder method Collation" in {
    val expected = Set("Builder")
    val local = getPublicFieldAndMethodNames(classOf[Collation.type])

    local should equal(expected)(after being normalized)
  }

  it should "return the underlying builder" in {
    Collation.builder().getClass should equal(classOf[com.mongodb.client.model.Collation.Builder])
  }

  it should "produce the same collation value when using the Scala helpers" in {
    val viaScalaHelper = Collation
      .builder()
      .backwards(true)
      .caseLevel(true)
      .collationAlternate(CollationAlternate.NON_IGNORABLE)
      .collationCaseFirst(CollationCaseFirst.UPPER)
      .collationMaxVariable(CollationMaxVariable.SPACE)
      .collationStrength(CollationStrength.TERTIARY)
      .locale("fr")
      .normalization(true)
      .numericOrdering(true)
      .build()

    val javaNative = JCollation
      .builder()
      .backwards(true)
      .caseLevel(true)
      .collationAlternate(JCollationAlternate.NON_IGNORABLE)
      .collationCaseFirst(JCollationCaseFirst.UPPER)
      .collationMaxVariable(JCollationMaxVariable.SPACE)
      .collationStrength(JCollationStrength.TERTIARY)
      .locale("fr")
      .normalization(true)
      .numericOrdering(true)
      .build()

    viaScalaHelper should equal(javaNative)
  }

}
