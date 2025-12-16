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

package com.mongodb.benchmark.jmh.codec;

import com.mongodb.internal.connection.BufferProvider;
import com.mongodb.internal.connection.ByteBufBsonDocument;
import com.mongodb.internal.connection.ByteBufferBsonOutput;
import com.mongodb.internal.connection.CompositeByteBuf;
import com.mongodb.lang.NonNull;
import org.bson.BsonArray;
import org.bson.BsonBinaryWriter;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.ByteBuf;
import org.bson.ByteBufNIO;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@Fork(value = 1)
@State(Scope.Benchmark)
public class ByteBufBsonDocumentBenchmark {

    public enum DocumentType {
        SIMPLE,
        COMPLEX,
        OP_MSG
    }

    @Param({"SIMPLE", "COMPLEX", "OP_MSG"})
    public DocumentType documentType;

    private ByteBufBsonDocument byteBufDocument;
    private ByteBuf byteBuf;
    private ByteBufferBsonOutput opMsgOutput;

    // Field names for lookups based on document type
    private String firstLevelFieldName;
    private String nestedDocFieldName;
    private String nestedArrayFieldName;

    @Setup(Level.Trial)
    public void setup() {
        switch (documentType) {
            case SIMPLE:
                setupSimpleDocument();
                break;
            case COMPLEX:
                setupComplexDocument();
                break;
            case OP_MSG:
                setupOpMsgDocument();
                break;
        }
    }

    private void setupSimpleDocument() {
        BsonDocument document = new BsonDocument()
                .append("_id", new BsonInt32(1))
                .append("name", new BsonString("test"))
                .append("value", new BsonInt32(42))
                .append("nested", new BsonDocument()
                        .append("x", new BsonInt32(100))
                        .append("y", new BsonString("nested value")))
                .append("items", new BsonArray(List.of(
                        new BsonInt32(1),
                        new BsonInt32(2),
                        new BsonDocument("arrayDoc", new BsonString("in array"))
                )));

        byteBuf = createByteBufFromDocument(document);
        byteBufDocument = new ByteBufBsonDocument(byteBuf);

        firstLevelFieldName = "name";
        nestedDocFieldName = "nested";
        nestedArrayFieldName = "items";
    }

    private void setupComplexDocument() {
        // Create deeply nested structure
        BsonDocument level3Doc = new BsonDocument()
                .append("level3Value", new BsonInt32(300))
                .append("level3String", new BsonString("deep nested"));

        BsonDocument level2Doc = new BsonDocument()
                .append("level2Value", new BsonInt32(200))
                .append("level3", level3Doc)
                .append("level2Array", new BsonArray(List.of(
                        new BsonDocument("arrayL2", new BsonInt32(21)),
                        new BsonDocument("arrayL2", new BsonInt32(22))
                )));

        BsonArray complexArray = new BsonArray();
        for (int i = 0; i < 10; i++) {
            complexArray.add(new BsonDocument()
                    .append("index", new BsonInt32(i))
                    .append("data", new BsonString("item " + i))
                    .append("nested", new BsonDocument("inner", new BsonInt32(i * 10))));
        }

        BsonDocument document = new BsonDocument()
                .append("_id", new BsonInt32(1))
                .append("type", new BsonString("complex"))
                .append("level1", level2Doc)
                .append("complexArray", complexArray)
                .append("metadata", new BsonDocument()
                        .append("created", new BsonInt32(1234567890))
                        .append("tags", new BsonArray(List.of(
                                new BsonString("tag1"),
                                new BsonString("tag2"),
                                new BsonString("tag3")
                        ))));

        byteBuf = createByteBufFromDocument(document);
        byteBufDocument = new ByteBufBsonDocument(byteBuf);

        firstLevelFieldName = "type";
        nestedDocFieldName = "level1";
        nestedArrayFieldName = "complexArray";
    }

    private void setupOpMsgDocument() {
        opMsgOutput = new ByteBufferBsonOutput(new SimpleBufferProvider());

        // Build the body document (the main command)
        BsonDocument bodyDoc = new BsonDocument()
                .append("insert", new BsonString("test"))
                .append("$db", new BsonString("db"));

        byte[] bodyBytes = encodeBsonDocument(bodyDoc);

        // Build sequence documents with nested structures
        List<byte[]> sequenceDocBytes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            BsonDocument seqDoc = new BsonDocument()
                    .append("_id", new BsonInt32(i))
                    .append("name", new BsonString("doc" + i))
                    .append("nested", new BsonDocument("value", new BsonInt32(i * 10)));
            sequenceDocBytes.add(encodeBsonDocument(seqDoc));
        }

        // Write OP_MSG format
        writeOpMsgFormat(opMsgOutput, bodyBytes, "documents", sequenceDocBytes);

        // Create ByteBufBsonDocument from the output
        List<ByteBuf> buffers = opMsgOutput.getByteBuffers();
        byteBufDocument = ByteBufBsonDocument.createCommandMessage(new CompositeByteBuf(buffers));

        firstLevelFieldName = "insert";
        nestedDocFieldName = "documents";
        nestedArrayFieldName = "documents";
    }

    private void writeOpMsgFormat(final ByteBufferBsonOutput output, final byte[] bodyBytes,
                                  final String sequenceIdentifier, final List<byte[]> sequenceDocBytes) {
        output.writeBytes(bodyBytes, 0, bodyBytes.length);

        int sequencePayloadSize = sequenceDocBytes.stream().mapToInt(b -> b.length).sum();
        int sequenceSectionSize = 4 + sequenceIdentifier.length() + 1 + sequencePayloadSize;

        output.writeByte(1);
        output.writeInt32(sequenceSectionSize);
        output.writeCString(sequenceIdentifier);
        for (byte[] docBytes : sequenceDocBytes) {
            output.writeBytes(docBytes, 0, docBytes.length);
        }
    }

    private static byte[] encodeBsonDocument(final BsonDocument doc) {
        try {
            BasicOutputBuffer buffer = new BasicOutputBuffer();
            new BsonDocumentCodec().encode(new BsonBinaryWriter(buffer), doc, EncoderContext.builder().build());
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            buffer.pipe(baos);
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Failed to encode BsonDocument", e);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
        if (byteBufDocument != null) {
            byteBufDocument.close();
        }
        if (byteBuf != null && documentType != DocumentType.OP_MSG) {
            byteBuf.release();
        }
        if (opMsgOutput != null) {
            opMsgOutput.close();
        }
    }

    // --- Benchmark Methods ---

    @Benchmark
    public String getFirstKey() {
        return byteBufDocument.getFirstKey();
    }

    @Benchmark
    public BsonValue lookupField() {
        return byteBufDocument.get(firstLevelFieldName);
    }

    @Benchmark
    public BsonValue lookupNestedDocument() {
        return byteBufDocument.get(nestedDocFieldName);
    }

    @Benchmark
    public void lookupMultipleNestedValues(Blackhole bh) {
        BsonValue nestedDoc = byteBufDocument.get(nestedDocFieldName);
        bh.consume(nestedDoc);

        if (nestedDoc != null && nestedDoc.isDocument()) {
            BsonDocument doc = nestedDoc.asDocument();
            for (String key : doc.keySet()) {
                bh.consume(doc.get(key));
            }
        }

        BsonValue arrayValue = byteBufDocument.get(nestedArrayFieldName);
        bh.consume(arrayValue);

        if (arrayValue != null && arrayValue.isArray()) {
            for (BsonValue element : arrayValue.asArray()) {
                bh.consume(element);
                if (element.isDocument()) {
                    for (Map.Entry<String, BsonValue> entry : element.asDocument().entrySet()) {
                        bh.consume(entry);
                    }
                }
            }
        }
    }

    @Benchmark
    public void iterateKeySet(Blackhole bh) {
        Set<String> keySet = byteBufDocument.keySet();
        for (String key : keySet) {
            bh.consume(key);
        }
    }

    @Benchmark
    public void iterateValueSet(Blackhole bh) {
        for (BsonValue value : byteBufDocument.values()) {
            bh.consume(value);
        }
    }

    @Benchmark
    public void iterateEntrySet(Blackhole bh) {
        for (Map.Entry<String, BsonValue> entry : byteBufDocument.entrySet()) {
            bh.consume(entry.getKey());
            bh.consume(entry.getValue());
        }
    }

    @Benchmark
    public BsonDocument toBsonDocument() {
        return byteBufDocument.toBsonDocument();
    }

    @Benchmark
    public boolean containsKey() {
        return byteBufDocument.containsKey(firstLevelFieldName);
    }

    @Benchmark
    public boolean containsKeyMissing() {
        return byteBufDocument.containsKey("nonExistentField");
    }

    @Benchmark
    public int size() {
        return byteBufDocument.size();
    }

    @Benchmark
    public boolean isEmpty() {
        return byteBufDocument.isEmpty();
    }

    // --- Helper Methods ---

    private static ByteBuf createByteBufFromDocument(final BsonDocument doc) {
        return new ByteBufNIO(ByteBuffer.wrap(encodeBsonDocument(doc)));
    }

    private static class SimpleBufferProvider implements BufferProvider {
        @NonNull
        @Override
        public ByteBuf getBuffer(final int size) {
            return new ByteBufNIO(ByteBuffer.allocate(size).order(ByteOrder.LITTLE_ENDIAN));
        }
    }
}
