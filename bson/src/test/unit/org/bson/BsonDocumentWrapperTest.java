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

package org.bson;

import org.bson.codecs.DocumentCodec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BsonDocumentWrapperTest {

    @Test
    @DisplayName("should serialize and deserialize")
    void shouldSerializeAndDeserialize() throws Exception {
        Document document = new Document()
                .append("a", 1)
                .append("b", 2)
                .append("c", Arrays.asList("x", true))
                .append("d", Arrays.asList(new Document("y", false), 1));

        BsonDocumentWrapper<Document> wrapper = new BsonDocumentWrapper<>(document, new DocumentCodec());

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(wrapper);

        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object deserializedDocument = ois.readObject();

        assertEquals(wrapper, deserializedDocument);
    }
}
