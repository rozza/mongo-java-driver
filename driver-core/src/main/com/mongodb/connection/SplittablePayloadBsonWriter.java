/*
 * Copyright 2017 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection;

import com.mongodb.client.model.SplittablePayload;
import org.bson.BsonBinaryWriter;


class SplittablePayloadBsonWriter extends LevelCountingBsonWriter {
    private final BsonBinaryWriter writer;
    private final BsonWriterHelper helper;
    private final SplittablePayload payload;
    private int commandStartPosition;

    SplittablePayloadBsonWriter(final BsonBinaryWriter writer, final MessageSettings settings, final SplittablePayload payload) {
        super(writer);
        this.writer = writer;
        this.helper = new BsonWriterHelper(writer, settings.getMaxBatchCount());
        this.payload = payload;
    }

    @Override
    public void writeStartDocument() {
        super.writeStartDocument();
        if (getCurrentLevel() == 0) {
            commandStartPosition = writer.getBsonOutput().getPosition();
        }
    }

    @Override
    public void writeEndDocument() {
        if (getCurrentLevel() == 0) {
            helper.writePayloadArray(payload, commandStartPosition);
        }
        super.writeEndDocument();
    }

}
