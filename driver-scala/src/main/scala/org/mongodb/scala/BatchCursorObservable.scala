/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala

/**
 * An Observable that also returns a BatchCursor
 * @tparam C the cursor type
 * @tparam T the main type of the observable
 */
trait BatchCursorObservable[C, T] extends Observable[T] {

  /**
   * Helper to return the first item from the Publisher.
   *
   * @return a publisher only containing the first item from the Publisher
   */
  def first: SingleObservable[T]

  /**
   * Sets the number of documents to return per batch.
   *
   * @param batchSize the batch size
   * @return this
   */
  def batchSize(batchSize: Int): Observable[T]

  /**
   * Gets the number of documents to return per batch
   *
   * @return the batch size
   */
  def getBatchSize: Option[Int]

  /**
   * Provide the underlying [[BatchCursor]] allowing fine grained control of the cursor.
   *
   * @return the Publisher containing the BatchCursor
   */
  def batchCursor: SingleObservable[C]
}
