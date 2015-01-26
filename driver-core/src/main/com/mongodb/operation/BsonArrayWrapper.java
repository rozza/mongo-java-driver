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

package com.mongodb.operation;

import org.bson.BsonArray;
import org.bson.BsonInvalidOperationException;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

import static org.bson.assertions.Assertions.notNull;


class BsonArrayWrapper<T> extends BsonArray {
    private static final long serialVersionUID = 3213553338060799471L;

    private final List<T> wrappedArray;
    private final transient Encoder<T> encoder;
    private BsonArray unwrapped;

    /**
     * Construct a new instance with the given array and encoder for the document.
     *
     * @param wrappedArray the wrapped array
     * @param encoder  the encoder for the wrapped array
     */
    public BsonArrayWrapper(final List<T> wrappedArray, final Encoder<T> encoder) {
        this.wrappedArray = notNull("wrappedArray", wrappedArray);
        this.encoder = encoder;
    }

    /**
     * Get the wrapped array.
     *
     * @return the wrapped array
     */
    public List<T> getWrappedArray() {
        return wrappedArray;
    }

    /**
     * Get the encoder to use for the wrapped document.
     *
     * @return the encoder
     */
    public Encoder<T> getEncoder() {
        return encoder;
    }

    /**
     * Determine whether the document has been unwrapped already.
     *
     * @return true if the wrapped array has been unwrapped already
     */
    public boolean isUnwrapped() {
        return unwrapped != null;
    }

    /**
     * Gets the values in this array as a list of {@code BsonValue} objects.
     *
     * @return the values in this array.
     */
    @Override
    public List<BsonValue> getValues() {
        return getUnwrapped().getValues();
    }

    @Override
    public int size() {
        return getUnwrapped().size();
    }

    @Override
    public boolean isEmpty() {
        return getUnwrapped().isEmpty();
    }

    @Override
    public boolean contains(final Object o) {
        return getUnwrapped().contains(o);
    }

    @Override
    public Iterator<BsonValue> iterator() {
        return getUnwrapped().iterator();
    }

    @Override
    public Object[] toArray() {
        return getUnwrapped().toArray();
    }

    @Override
    public <T> T[] toArray(final T[] a) {
        return getUnwrapped().toArray(a);
    }

    @Override
    public boolean add(final BsonValue bsonValue) {
        return getUnwrapped().add(bsonValue);
    }

    @Override
    public boolean remove(final Object o) {
        return getUnwrapped().remove(o);
    }

    @Override
    public boolean containsAll(final Collection<?> c) {
        return getUnwrapped().containsAll(c);
    }

    @Override
    public boolean addAll(final Collection<? extends BsonValue> c) {
        return getUnwrapped().addAll(c);
    }

    @Override
    public boolean addAll(final int index, final Collection<? extends BsonValue> c) {
        return getUnwrapped().addAll(index, c);
    }

    @Override
    public boolean removeAll(final Collection<?> c) {
        return getUnwrapped().removeAll(c);
    }

    @Override
    public boolean retainAll(final Collection<?> c) {
        return getUnwrapped().retainAll(c);
    }

    @Override
    public void clear() {
        getUnwrapped().clear();
    }

    @Override
    public BsonValue get(final int index) {
        return getUnwrapped().get(index);
    }

    @Override
    public BsonValue set(final int index, final BsonValue element) {
        return getUnwrapped().set(index, element);
    }

    @Override
    public void add(final int index, final BsonValue element) {
        getUnwrapped().add(index, element);
    }

    @Override
    public BsonValue remove(final int index) {
        return getUnwrapped().remove(index);
    }

    @Override
    public int indexOf(final Object o) {
        return getUnwrapped().indexOf(o);
    }

    @Override
    public int lastIndexOf(final Object o) {
        return getUnwrapped().lastIndexOf(o);
    }

    @Override
    public ListIterator<BsonValue> listIterator() {
        return getUnwrapped().listIterator();
    }

    @Override
    public ListIterator<BsonValue> listIterator(final int index) {
        return getUnwrapped().listIterator(index);
    }

    @Override
    public List<BsonValue> subList(final int fromIndex, final int toIndex) {
        return getUnwrapped().subList(fromIndex, toIndex);
    }

    @Override
    public boolean equals(final Object o) {
        return getUnwrapped().equals(o);
    }

    @Override
    public int hashCode() {
        return getUnwrapped().hashCode();
    }

    @Override
    public String toString() {
        return getUnwrapped().toString();
    }

    private BsonArray getUnwrapped() {
        if (encoder == null) {
            throw new BsonInvalidOperationException("Can not unwrap a BsonDocumentWrapper with no Encoder");
        }
        if (unwrapped == null) {
            BsonArray unwrapped = new BsonArray();
            BsonWriter writer = new BsonArrayWriter(unwrapped);
            for (T arrayItem : wrappedArray) {
                encoder.encode(writer, arrayItem, EncoderContext.builder().build());
            }
            this.unwrapped = unwrapped;
        }
        return unwrapped;
    }
}
