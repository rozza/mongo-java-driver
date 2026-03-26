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

package org.bson.internal;

import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonWriter;
import org.bson.ByteBufNIO;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.MinKeyCodec;
import org.bson.codecs.configuration.CodecConfigurationException;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.io.ByteBufferBsonInput;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProvidersCodecRegistryTest {

    @Test
    void shouldThrowIfSuppliedCodecProvidersIsNull() {
        assertThrows(IllegalArgumentException.class, () -> new ProvidersCodecRegistry(null));
    }

    @Test
    void shouldThrowIfSuppliedCodecProvidersIsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new ProvidersCodecRegistry(Collections.emptyList()));
    }

    @Test
    void shouldThrowCodecConfigurationExceptionIfCodecNotFound() {
        assertThrows(CodecConfigurationException.class, () ->
                new ProvidersCodecRegistry(Collections.singletonList(new SingleCodecProvider(new MinKeyCodec()))).get(MaxKey.class));
    }

    @Test
    void getShouldReturnRegisteredCodec() {
        MinKeyCodec minKeyCodec = new MinKeyCodec();
        ProvidersCodecRegistry registry = new ProvidersCodecRegistry(Collections.singletonList(new SingleCodecProvider(minKeyCodec)));
        assertSame(minKeyCodec, registry.get(MinKey.class));
    }

    @Test
    void getShouldReturnCodecFromFirstSourceThatHasOne() {
        MinKeyCodec minKeyCodec1 = new MinKeyCodec();
        MinKeyCodec minKeyCodec2 = new MinKeyCodec();
        ProvidersCodecRegistry registry = new ProvidersCodecRegistry(
                Arrays.asList(new SingleCodecProvider(minKeyCodec1), new SingleCodecProvider(minKeyCodec2)));
        assertSame(minKeyCodec1, registry.get(MinKey.class));
    }

    @Test
    void shouldHandleCycles() throws IOException {
        ProvidersCodecRegistry registry = new ProvidersCodecRegistry(Collections.singletonList(new ClassModelCodecProvider()));
        @SuppressWarnings("unchecked")
        Codec<Top> topCodec = (Codec<Top>) registry.get(Top.class);
        assertInstanceOf(TopCodec.class, topCodec);

        Top top = new Top("Bob",
                new Top("Jim", null, null),
                new Nested("George", new Top("Joe", null, null)));
        BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
        BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);
        topCodec.encode(writer, top, EncoderContext.builder().build());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        outputBuffer.pipe(os);
        writer.close();

        Top decoded = topCodec.decode(
                new BsonBinaryReader(new ByteBufferBsonInput(new ByteBufNIO(ByteBuffer.wrap(os.toByteArray())))),
                DecoderContext.builder().build());
        assertEquals(top, decoded);
    }

    private static final CodecRegistry DUMMY_REGISTRY = new CodecRegistry() {
        @Override
        public <T> Codec<T> get(final Class<T> clazz) {
            throw new CodecConfigurationException("No codec for " + clazz);
        }

        @Override
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            return null;
        }
    };

    @Test
    void getShouldUseTheCodecCache() {
        Codec<MinKey> codec = new MinKeyCodec();
        CodecProvider provider = new CodecProvider() {
            private int counter = 0;

            @Override
            @SuppressWarnings("unchecked")
            public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
                if (counter == 0) {
                    counter++;
                    return (Codec<T>) codec;
                }
                throw new AssertionError("Must not be called more than once.");
            }
        };

        ProvidersCodecRegistry registry = new ProvidersCodecRegistry(Collections.singletonList(provider));
        Codec<MinKey> codecFromRegistry = registry.get(MinKey.class);
        assertEquals(codec, codecFromRegistry);

        codecFromRegistry = registry.get(MinKey.class);
        assertEquals(codec, codecFromRegistry);
    }

    @Test
    void getWithCodecRegistryShouldReturnCodecFromFirstSourceThatHasOne() {
        ProvidersCodecRegistry provider = new ProvidersCodecRegistry(
                Collections.singletonList(new ClassModelCodecProvider(Arrays.asList(Simple.class))));
        CodecRegistry registry = DUMMY_REGISTRY;
        assertInstanceOf(SimpleCodec.class, provider.get(Simple.class, registry));
    }

    @Test
    void getWithCodecRegistryShouldReturnNullIfCodecNotFound() {
        ProvidersCodecRegistry provider = new ProvidersCodecRegistry(
                Collections.singletonList(new ClassModelCodecProvider(Arrays.asList(Top.class))));
        CodecRegistry registry = DUMMY_REGISTRY;
        assertNull(provider.get(Simple.class, registry));
    }

    @Test
    void getWithCodecRegistryShouldPassOuterRegistryToProviders() {
        ProvidersCodecRegistry provider = new ProvidersCodecRegistry(
                Collections.singletonList(new ClassModelCodecProvider(Arrays.asList(Simple.class))));
        CodecRegistry registry = DUMMY_REGISTRY;
        SimpleCodec simpleCodec = (SimpleCodec) provider.get(Simple.class, registry);
        assertSame(registry, simpleCodec.getRegistry());
    }

    // Helper classes

    static class SingleCodecProvider implements CodecProvider {
        private final Codec<?> codec;

        SingleCodecProvider(final Codec<?> codec) {
            this.codec = codec;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            if (clazz == codec.getEncoderClass()) {
                return (Codec<T>) codec;
            }
            return null;
        }
    }

    static class ClassModelCodecProvider implements CodecProvider {
        private final List<Class<?>> supportedClasses;

        ClassModelCodecProvider() {
            this(Arrays.asList(Top.class, Nested.class));
        }

        ClassModelCodecProvider(final List<Class<?>> supportedClasses) {
            this.supportedClasses = supportedClasses;
        }

        @Override
        @SuppressWarnings("unchecked")
        public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
            if (!supportedClasses.contains(clazz)) {
                return null;
            } else if (clazz == Top.class) {
                try {
                    return (Codec<T>) new TopCodec(registry);
                } catch (CodecConfigurationException e) {
                    return null;
                }
            } else if (clazz == Nested.class) {
                try {
                    return (Codec<T>) new NestedCodec(registry);
                } catch (CodecConfigurationException e) {
                    return null;
                }
            } else if (clazz == Simple.class) {
                return (Codec<T>) new SimpleCodec(registry);
            }
            return null;
        }
    }

    static class TopCodec implements Codec<Top> {
        private final Codec<Top> codecForOther;
        private final Codec<Nested> codecForNested;
        private final CodecRegistry registry;

        @SuppressWarnings("unchecked")
        TopCodec(final CodecRegistry registry) {
            this.registry = registry;
            this.codecForOther = (Codec<Top>) registry.get(Top.class);
            this.codecForNested = (Codec<Nested>) registry.get(Nested.class);
        }

        @Override
        public void encode(final BsonWriter writer, final Top top, final EncoderContext encoderContext) {
            if (top == null) {
                writer.writeNull();
                return;
            }
            writer.writeStartDocument();
            writer.writeString("name", top.getName());
            writer.writeName("other");
            codecForOther.encode(writer, top.getOther(), EncoderContext.builder().build());
            writer.writeName("nested");
            codecForNested.encode(writer, top.getNested(), EncoderContext.builder().build());
            writer.writeEndDocument();
        }

        @Override
        public Class<Top> getEncoderClass() {
            return Top.class;
        }

        @Override
        public Top decode(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readStartDocument();
            reader.readName();
            String name = reader.readString();
            Top other = null;
            Nested nested = null;

            BsonType type = reader.readBsonType();
            reader.readName();
            if (type == BsonType.NULL) {
                reader.readNull();
            } else {
                other = codecForOther.decode(reader, decoderContext);
            }

            reader.readName("nested");
            if (type == BsonType.NULL) {
                reader.readNull();
            } else {
                nested = codecForNested.decode(reader, decoderContext);
            }
            reader.readEndDocument();
            return new Top(name, other, nested);
        }
    }

    static class NestedCodec implements Codec<Nested> {
        private final Codec<Top> codecForTop;

        @SuppressWarnings("unchecked")
        NestedCodec(final CodecRegistry registry) {
            this.codecForTop = (Codec<Top>) registry.get(Top.class);
        }

        @Override
        public void encode(final BsonWriter writer, final Nested nested, final EncoderContext encoderContext) {
            if (nested == null) {
                writer.writeNull();
                return;
            }
            writer.writeStartDocument();
            writer.writeString("name", nested.getName());
            writer.writeName("top");
            codecForTop.encode(writer, nested.getTop(), EncoderContext.builder().build());
            writer.writeEndDocument();
        }

        @Override
        public Class<Nested> getEncoderClass() {
            return Nested.class;
        }

        @Override
        public Nested decode(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readStartDocument();
            reader.readName();
            String name = reader.readString();
            BsonType type = reader.readBsonType();
            reader.readName();
            Top top = null;
            if (type == BsonType.NULL) {
                reader.readNull();
            } else {
                top = codecForTop.decode(reader, decoderContext);
            }
            reader.readEndDocument();
            return new Nested(name, top);
        }
    }

    static class SimpleCodec implements Codec<Simple> {
        private final CodecRegistry registry;

        SimpleCodec(final CodecRegistry registry) {
            this.registry = registry;
        }

        CodecRegistry getRegistry() {
            return registry;
        }

        @Override
        public void encode(final BsonWriter writer, final Simple value, final EncoderContext encoderContext) {
            writer.writeNull();
        }

        @Override
        public Class<Simple> getEncoderClass() {
            return Simple.class;
        }

        @Override
        public Simple decode(final BsonReader reader, final DecoderContext decoderContext) {
            reader.readNull();
            return new Simple();
        }
    }

    static class Top {
        private final String name;
        private final Top other;
        private final Nested nested;

        Top(final String name, final Top other, final Nested nested) {
            this.name = name;
            this.other = other;
            this.nested = nested;
        }

        String getName() {
            return name;
        }

        Top getOther() {
            return other;
        }

        Nested getNested() {
            return nested;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Top top = (Top) o;
            return Objects.equals(name, top.name)
                    && Objects.equals(other, top.other)
                    && Objects.equals(nested, top.nested);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, other, nested);
        }
    }

    static class Nested {
        private final String name;
        private final Top top;

        Nested(final String name, final Top top) {
            this.name = name;
            this.top = top;
        }

        String getName() {
            return name;
        }

        Top getTop() {
            return top;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Nested nested = (Nested) o;
            return Objects.equals(name, nested.name)
                    && Objects.equals(top, nested.top);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, top);
        }
    }

    static class Simple {
        int value = 0;
    }
}
