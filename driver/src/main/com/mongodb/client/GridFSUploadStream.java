/*
 * Copyright 2015 MongoDB, Inc.
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

package com.mongodb.client;

import org.bson.types.ObjectId;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A GridFS OutputStream for uploading data into GridFS
 *
 * Provides the {@code fileId} for the file to be uploaded as well as the {@code write} methods of an {@link OutputStream}
 *
 * @since 3.1
 */
public interface GridFSUploadStream {

    /**
     * Gets the {@link ObjectId} for the file to be uploaded
     *
     * @return the ObjectId for the file to be uploaded
     */
    ObjectId getFileId();

    /**
     * Gets the {@link OutputStream} for uploading data into GridFS
     *
     * @return the OutputStream for uploading data into GridFS
     */
    OutputStream getOutputStream();

    /**
     * Writes the specified byte to this output stream. The general contract for {@code write} is that one byte is written
     * to the output stream. The byte to be written is the eight low-order bits of the argument {@code b}. The 24 high-order bits of
     * {@code b} are ignored.
     *
     * @param b the {@code byte}.
     * @throws IOException if an I/O error occurs. In particular,
     *         an {@code IOException} may be thrown if the output stream has been closed.
     */
    void write(int b) throws IOException;

    /**
     * Writes {@code b.length} bytes from the specified byte array to this output stream. The general contract for {@code write(b)}
     * is that it should have exactly the same effect as the call {@code write(b, 0, b.length)}.
     *
     * @param b the data.
     * @throws IOException if an I/O error occurs.
     * @see java.io.OutputStream#write(byte[], int, int)
     */
    void write(byte[] b) throws IOException;

    /**
     * Writes {@code len} bytes from the specified byte array starting at offset {@code off} to this output stream.
     * The general contract for {@code write(b, off, len)} is that some of the bytes in the array {@code b} are written to the
     * output stream in order; element {@code b[off]} is the first byte written and {@code b[off+len-1]} is the last byte written
     * by this operation.
     * <p>
     * The {@code write} method of {@code OutputStream} calls the write method of one argument on each of the bytes to be
     * written out. Subclasses are encouraged to override this method and provide a more efficient implementation.
     * </p>
     * <p>
     * If {@code b} is {@code null}, a
     * {@code NullPointerException} is thrown.
     * </p>
     * <p>
     * If {@code off} is negative, or {@code len} is negative, or {@code off+len} is greater than the length of the array {@code b},
     * then an <em>IndexOutOfBoundsException</em> is thrown.
     * </p>
     *
     * @param b   the data.
     * @param off the start offset in the data.
     * @param len the number of bytes to write.
     * @throws IOException if an I/O error occurs. In particular, an {@code IOException} is thrown if the output stream is closed.
     */
    void write(byte[] b, int off, int len) throws IOException;

    /**
     * The GridFS specification requires all chunk sizes to be the same apart from the last chunk. As manual flushing would not follow the
     * specification in this implementation it does nothing.
     *
     * @throws IOException if an I/O error occurs.
     */
    void flush() throws IOException;

    /**
     * Closes this output stream and releases any system resources associated with this stream. The general contract of {@code close}
     * is that it closes the output stream. A closed stream cannot perform output operations and cannot be reopened.
     *
     * @throws IOException if an I/O error occurs.
     */
    void close() throws IOException;

}
