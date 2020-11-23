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
package org.mongodb.scala.internal

import java.io.Closeable

import com.mongodb.reactivestreams.client.{ AggregateBatchCursor => JAggregationBatchCursor }
import org.bson.{ BsonDocument, BsonTimestamp }
import org.mongodb.scala.{ AggregationBatchCursor, Observable, ToSingleObservablePublisher }

import scala.collection.JavaConverters._

case class AggregationBatchCursorImpl[T](wrapped: JAggregationBatchCursor[T])
    extends AggregationBatchCursor[T]
    with Closeable {

  def next(): Observable[List[T]] = wrapped.next().toSingle().map(_.asScala.toList)

  def tryNext(): Observable[List[T]] = wrapped.tryNext().toSingle().map(_.asScala.toList)

  def setBatchSize(batchSize: Int): Unit = wrapped.setBatchSize(batchSize)

  def getBatchSize: Int = wrapped.getBatchSize

  def isClosed: Boolean = wrapped.isClosed

  def getPostBatchResumeToken: BsonDocument = wrapped.getPostBatchResumeToken

  def getOperationTime: BsonTimestamp = wrapped.getOperationTime

  def isFirstBatchEmpty: Boolean = wrapped.isFirstBatchEmpty

  def getMaxWireVersion: Int = wrapped.getMaxWireVersion

  override def close(): Unit = wrapped.close()

}
