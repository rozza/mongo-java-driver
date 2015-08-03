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

package com.mongodb.client.gridfs;

import org.bson.BsonDocument;

import java.io.IOException;
import java.io.InputStream;

/**
 * A GridFS InputStream for downloading data from GridFS
 *
 * @since 3.1
 */
public interface GridFSDownloadStream {

    /**
     * Gets the corresponding files collection document for the file being downloaded
     *
     * @return the corresponding files collection document for the file being downloaded
     */
    BsonDocument getFileInformation();

    /**
     * Gets the {@link InputStream} for downloading data from GridFS
     *
     * @return the downloading for downloading data form GridFS
     */
    InputStream getInputStream();

    /**
     * Reads the next byte of data from the input stream. The value byte is returned as an {@code int} in the range {@code 0} to
     * {@code 255}. If no byte is available because the end of the stream has been reached, the value {@code -1} is returned.
     *
     * <p>This method blocks until input data is available, the end of the stream is detected, or an exception is thrown.</p>
     *
     * @return the next byte of data, or {@code -1} if the end of the stream is reached.
     * @throws IOException if an I/O error occurs.
     */
    int read() throws IOException;

    /**
     * Reads some number of bytes from the input stream and stores them into the buffer array {@code b}. The number of bytes actually read
     * is returned as an integer. This method blocks until input data is available, end of file is detected, or an exception is thrown.
     *
     * <p>If the length of {@code b} is zero, then no bytes are read and {@code 0} is returned; otherwise, there is an attempt to read at
     * least one byte. If no byte is available because the stream is at the end of the file, the value {@code -1} is returned; otherwise,
     * at least one byte is read and stored into {@code b}.</p>
     *
     * <p>The first byte read is stored into element {@code b[0]}, the next one into {@code b[1]}, and so on. The number of bytes read is,
     * at most, equal to the length of {@code b}. Let <em>k</em> be the number of bytes actually read; these bytes will be stored in
     * elements {@code b[0]} through {@code b[}<em>k</em>{@code -1]}, leaving elements {@code b[}<em>k</em>{@code ]} through
     * {@code b[b.length-1]} unaffected.</p>
     *
     * <p>The {@code read(b)} method for class {@code InputStream} has the same effect as: {@code  read(b, 0, b.length) }</p>
     *
     * @param b the buffer into which the data is read.
     * @return the total number of bytes read into the buffer, or {@code -1} if there is no more data because the end of
     * the stream has been reached.
     * @throws IOException          If the first byte cannot be read for any reason other than the end of the file, if the input stream has
     *                              been closed, or if some other I/O error occurs.
     * @throws NullPointerException if {@code b} is {@code null}.
     * @see java.io.InputStream#read(byte[], int, int)
     */
    int read(byte[] b) throws IOException;

    /**
     * Reads up to {@code len} bytes of data from the input stream into an array of bytes.  An attempt is made to read as many as
     * {@code len} bytes, but a smaller number may be read. The number of bytes actually read is returned as an integer.
     *
     * <p> This method blocks until input data is available, end of file is detected, or an exception is thrown.</p>
     *
     * <p> If {@code len} is zero, then no bytes are read and {@code 0} is returned; otherwise, there is an attempt to read at least one
     * byte. If no byte is available because the stream is at end of file, the value {@code -1} is returned; otherwise, at least one byte
     * is read and stored into {@code b}.</p>
     *
     * <p> The first byte read is stored into element {@code b[off]}, the next one into {@code b[off+1]}, and so on. The number of bytes
     * read is, at most, equal to {@code len}. Let <em>k</em> be the number of bytes actually read; these bytes will be stored in elements
     * {@code b[off]} through {@code b[off+}<em>k</em>{@code -1]}, leaving elements {@code b[off+}<em>k</em>{@code ]} through
     * {@code b[off+len-1]} unaffected.</p>
     *
     * <p> In every case, elements {@code b[0]} through {@code b[off]} and elements {@code b[off+len]} through
     * {@code b[b.length-1]} are unaffected.</p>
     *
     * <p> The {@code read(b,} {@code off,} {@code len)} method for class {@code InputStream} simply calls the method
     * {@code read()} repeatedly. If the first such call results in an {@code IOException}, that exception is returned from the call to
     * the {@code read(b,} {@code off,} {@code len)} method. If any subsequent call to {@code read()} results in a {@code IOException},
     * the exception is caught and treated as if it were end of file; the bytes read up to that point are stored into {@code b} and the
     * number of bytes read before the exception occurred is returned. The default implementation of this method blocks until the requested
     * amount of input data {@code len} has been read, end of file is detected, or an exception is thrown.</p>
     *
     * @param b   the buffer into which the data is read.
     * @param off the start offset in array {@code b} at which the data is written.
     * @param len the maximum number of bytes to read.
     * @return the total number of bytes read into the buffer, or {@code -1} if there is no more data because the end of the stream has
     * been reached.
     * @throws IOException               If the first byte cannot be read for any reason other than end of file, or if the input stream
     *                                   has been closed, or if some other I/O error occurs.
     * @throws NullPointerException      If {@code b} is {@code null}.
     * @throws IndexOutOfBoundsException If {@code off} is negative, {@code len} is negative, or {@code len} is greater than
     *                                   {@code b.length - off}
     * @see java.io.InputStream#read()
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Skips over and discards {@code n} bytes of data from this input stream. The {@code skip} method may, for a variety of reasons, end
     * up skipping over some smaller number of bytes, possibly {@code 0}. This may result from any of a number of conditions; reaching end
     * of file before {@code n} bytes have been skipped is only one possibility. The actual number of bytes skipped is returned.
     * If {@code n} is negative, the {@code skip} method for class {@code InputStream} always returns 0, and no bytes are skipped.</p>
     *
     * @param bytesToSkip the number of bytes to be skipped.
     * @return the actual number of bytes skipped.
     * @throws IOException if the stream does not support seek, or if some other I/O error occurs.
     */
    long skip(long bytesToSkip) throws IOException;

    /**
     * Returns an estimate of the number of bytes that can be read (or skipped over) from this input stream without blocking by the next
     * invocation of a method for this input stream. The next invocation might be the same thread or another thread.
     * A single read or skip of this many bytes will not block, but may read or skip fewer bytes.
     *
     * @return an estimate of the number of bytes that can be read (or skipped over) from this input stream without blocking or
     * {@code 0} when it reaches the end of the input stream.
     * @throws IOException if an I/O error occurs.
     */
    int available() throws IOException;

    /**
     * Closes this input stream and releases any system resources associated with the stream.
     *
     * @throws IOException if an I/O error occurs.
     */
    void close() throws IOException;

}
