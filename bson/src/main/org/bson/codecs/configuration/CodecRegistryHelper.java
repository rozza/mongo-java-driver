/*
 * Copyright 2015 MongoDB, Inc.
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

package org.bson.codecs.configuration;

import org.bson.codecs.Codec;

import java.util.List;

import static java.util.Arrays.asList;

/**
 * A helper class for creating and combining codec registries
 */
public final class CodecRegistryHelper {

    /**
     * Creates a codec registry from the provided codec
     *
     * <p>This registry can then be used alongside other registries.  Typically used when adding extra codecs to existing codecs with the
     * {@link this#preferredRegistry} helper.</p>
     *
     * @param codec the codec to create a registry for
     * @param <T> the value type of the codec
     * @return a codec registry for the given codec.
     */
    public static <T> CodecRegistry fromCodec(final Codec<T> codec) {
        return new SimpleCodecRegistry<T>(codec);
    }

    /**
     *  A codec registry that uses a codec provider to look up codecs for {@see fromProviders}
     *
     * @param codecProvider the codec provider
     * @return a codec registry that uses a codec provider to find codecs
     */
    public static CodecRegistry fromProvider(final CodecProvider codecProvider) {
        return fromProviders(asList(codecProvider));
    }

    /**
     * A codec registry that contains a list of providers to look up codecs for.
     *
     * This class can handle cycles of Codec dependencies, i.e when the construction of a Codec for class A requires the construction of a
     * Codec for class B, and vice versa.
     *
     * @param codecProviders the list of codec providers
     * @return a codec registry that has an ordered list of codec providers.
     */
    public static CodecRegistry fromProviders(final List<CodecProvider> codecProviders) {
        return new ProviderCodecRegistry(codecProviders);
    }

    /**
     * A codec registry that contains two possible registries when looking for codecs.
     *
     * <p>The preferred registry is always checked first when looking up codecs and if that returns null it falls back to the alternative
     * registry. Typically used when extending existing codec registries with new registries.</p>
     *
     *
     * @param preferredRegistry the preferred registry for codec lookups
     * @param alternativeRegistry the fallback registry for codec lookups.
     *
     * @return a codec registry that has a preferred registry when looking for codecs.
     */
    public static CodecRegistry preferredRegistry(final CodecRegistry preferredRegistry, final CodecRegistry alternativeRegistry) {
        return new PreferredCodecRegistry(preferredRegistry, alternativeRegistry);
    }

    private CodecRegistryHelper() {
    }
}
