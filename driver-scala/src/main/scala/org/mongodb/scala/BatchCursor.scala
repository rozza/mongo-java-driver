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

import com.mongodb.reactivestreams.client.{ BatchCursor => JBatchCursor }
import org.mongodb.scala.internal.BatchCursorImpl

/**
 * Batch cursor companion object
 */
object BatchCursor {
  def apply[T](wrapped: JBatchCursor[T]): BatchCursor[T] = BatchCursorImpl(wrapped)
}

/**
 * MongoDB returns query results as batches, and this interface provides an asynchronous iterator over those batches.  The first call to
 * the `next` method will return the first batch, and subsequent calls will trigger an asynchronous request to get the next batch
 * of results.  Clients can control the batch size by setting the `batchSize` property between calls to `next`.
 *
 * @tparam T The type of documents the cursor contains
 * @mongodb.driver.manual ../meta-driver/latest/legacy/mongodb-wire-protocol/#wire-op-get-more OP_GET_MORE
 * @since 4.2
 */
trait BatchCursor[T] {

  /**
   * Returns the next batch of results. A tailable cursor will internally block until another batch exists.
   *
   * @return a single item observable containing the next elements
   */
  def next(): Observable[List[T]]

  /**
   * A special `next` case that returns the next batch if available or just completes.
   *
   * Tailable cursors are an example where this is useful. A call to `tryNext` may return no results, but in the future calling
   * `tryNext` would return a new batch if a document had been added to the capped collection.
   *
   * @return a single item observable containing the next elements
   */
  def tryNext(): Observable[List[T]]

  /**
   * Sets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
   *
   * @param batchSize the non-negative batch size.  0 means to use the server default.
   */
  def setBatchSize(batchSize: Int): Unit

  /**
   * Gets the batch size to use when requesting the next batch.  This is the number of documents to request in the next batch.
   *
   * @return the non-negative batch size.  0 means to use the server default.
   */
  def getBatchSize: Int

  /**
   * Return true if the BatchCursor has been closed
   *
   * @return true if the BatchCursor has been closed
   */
  def isClosed: Boolean

  def close(): Unit

}
