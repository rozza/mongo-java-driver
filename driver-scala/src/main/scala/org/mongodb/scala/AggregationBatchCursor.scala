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

package org.mongodb.scala

import com.mongodb.reactivestreams.client.{ AggregateBatchCursor => JAggregationBatchCursor }
import org.bson.{ BsonDocument, BsonTimestamp }
import org.mongodb.scala.internal.AggregationBatchCursorImpl

/**
 * Aggregation Batch cursor companion object
 */
object AggregationBatchCursor {
  def apply[T](wrapped: JAggregationBatchCursor[T]): AggregationBatchCursor[T] = AggregationBatchCursorImpl(wrapped)
}

/**
 * An extended batch cursor interface that includes information included in an aggregate or getMore response.
 *
 * @tparam T The type of documents the cursor contains
 * @since 4.2
 */
trait AggregationBatchCursor[T] extends BatchCursor[T] {

  /**
   * Returns the postBatchResumeToken.
   *
   * @return the postBatchResumeToken
   */
  def getPostBatchResumeToken: BsonDocument

  /**
   * Returns the operation time found in the aggregate or getMore response.
   *
   * @return the operation time
   */
  def getOperationTime: BsonTimestamp

  /**
   * Returns true if the first batch was empty.
   *
   * @return true if the first batch was empty
   */
  def isFirstBatchEmpty: Boolean

  /**
   * Returns the max wire version.
   *
   * @return the max wire version
   */
  def getMaxWireVersion: Int

}
