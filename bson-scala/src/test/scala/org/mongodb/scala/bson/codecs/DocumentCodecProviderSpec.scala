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

package org.mongodb.scala.bson.codecs

import org.bson.codecs.configuration.CodecRegistries.fromProviders

import org.mongodb.scala.bson.collection.{ Document, immutable, mutable }
import org.scalatest.{ FlatSpec, Matchers }

class DocumentCodecProviderSpec extends FlatSpec with Matchers {

  "DocumentCodecProvider" should "get the correct codec" in {

    val provider = DocumentCodecProvider()
    val registry = fromProviders(provider)

    provider.get[Document](classOf[Document], registry) shouldBe a[ImmutableDocumentCodec]
    provider.get[immutable.Document](classOf[immutable.Document], registry) shouldBe a[ImmutableDocumentCodec]
    provider.get[mutable.Document](classOf[mutable.Document], registry) shouldBe a[MutableDocumentCodec]
    Option(provider.get[String](classOf[String], registry)) shouldBe None
  }
}
