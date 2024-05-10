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

import org.mongodb.scala.BaseSpec
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.util.{ Success, Try }

class BucketGranularitySpec extends BaseSpec {

  "BucketGranularity" should "have the same static fields as the wrapped BucketGranularity" in {

    val wrapped = getPublicFieldAndMethodNames(classOf[BucketGranularity])
    val local = getPublicFieldAndMethodNames(classOf[BucketGranularity.type])

    local should equal(wrapped)(after being normalized)
  }

  it should "return the expected BucketGranularity" in {
    forAll(BucketGranularitys) { (value: String, expectedValue: Try[BucketGranularity]) =>
      BucketGranularity.fromString(value) should equal(expectedValue)
    }
  }

  it should "handle invalid values" in {
    forAll(invalidBucketGranularitys) { (value: String) =>
      BucketGranularity.fromString(value) should be a Symbol("failure")
    }
  }

  val BucketGranularitys =
    Table(
      ("stringValue", "JavaValue"),
      ("R5", Success(BucketGranularity.R5)),
      ("R10", Success(BucketGranularity.R10)),
      ("R20", Success(BucketGranularity.R20)),
      ("R40", Success(BucketGranularity.R40)),
      ("R80", Success(BucketGranularity.R80)),
      ("1-2-5", Success(BucketGranularity.SERIES_125)),
      ("E6", Success(BucketGranularity.E6)),
      ("E12", Success(BucketGranularity.E12)),
      ("E24", Success(BucketGranularity.E24)),
      ("E48", Success(BucketGranularity.E48)),
      ("E96", Success(BucketGranularity.E96)),
      ("E192", Success(BucketGranularity.E192)),
      ("POWERSOF2", Success(BucketGranularity.POWERSOF2))
    )

  val invalidBucketGranularitys = Table("invalid values", "r5", "powers of 2")
}
