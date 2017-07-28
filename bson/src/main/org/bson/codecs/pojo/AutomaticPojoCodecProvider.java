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

package org.bson.codecs.pojo;

import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;

/**
 * An Automatic PojoCodec Provider
 *
 * <p>Creates {@link PojoCodecImpl} instances for any classes that have at least one property, using the default configuration.</p>
 *
 * @since 3.5
 */
public final class AutomaticPojoCodecProvider implements CodecProvider {
    private final DiscriminatorLookup discriminatorLookup = new DiscriminatorLookup();

    @Override
    public <T> Codec<T> get(final Class<T> clazz, final CodecRegistry registry) {
        ClassModel<T> classModel;
        try {
            classModel = ClassModel.builder(clazz).build();
        } catch (IllegalStateException e) {
            return null;
        }
        if (classModel.getPropertyModels().isEmpty()) {
            return null;
        }
        discriminatorLookup.addClassModel(classModel);
        return new AutomaticPojoCodec<T>(new PojoCodecImpl<T>(classModel, registry, discriminatorLookup));
    }

}
