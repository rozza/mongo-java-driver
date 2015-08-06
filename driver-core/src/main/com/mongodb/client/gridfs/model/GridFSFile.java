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

package com.mongodb.client.gridfs.model;

import org.bson.BsonValue;
import org.bson.Document;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.List;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The GridFSFile
 *
 * @since 3.1
 */
public final class GridFSFile {
    private final BsonValue id;
    private final String filename;
    private final long length;
    private final int chunkSize;
    private final Date uploadDate;
    private final String md5;

    // Optional values
    private final Document metadata;

    // Deprecated values
    private final String contentType;
    private final List<String> aliases;

    /**
     * Creates a new GridFSFile
     *
     * @param id the id of the file
     * @param filename the filename
     * @param length the length, in bytes of the file
     * @param chunkSize the chunkSize, in bytes of the file
     * @param uploadDate the upload date of the file
     * @param md5 the hash of the files contents
     * @param metadata the optional metadata for the file
     */
    public GridFSFile(final BsonValue id, final String filename, final long length, final int chunkSize, final Date uploadDate,
                      final String md5, final Document metadata) {
        this(id, filename, length, chunkSize, uploadDate, md5, metadata, null, null);
    }

    /**
     * Creates a legacy implementation of the GridFSFile
     *
     * <p>For GridFS files created in older versions of the driver.</p>
     *
     * @param id the id of the file
     * @param filename the filename
     * @param length the length, in bytes of the file
     * @param chunkSize the chunkSize, in bytes of the file
     * @param uploadDate the upload date of the file
     * @param md5 the hash of the files contents
     * @param metadata the optional metadata for the file
     * @param contentType the optional contentType for the file
     * @param aliases the optional aliases for the file
     */
    public GridFSFile(final BsonValue id, final String filename, final long length, final int chunkSize, final Date uploadDate,
                      final String md5, final Document metadata, final String contentType, final List<String> aliases) {
        this.id = notNull("id", id);
        this.filename = notNull("filename", filename);
        this.length = notNull("length", length);
        this.chunkSize = notNull("chunkSize", chunkSize);
        this.uploadDate = notNull("uploadDate", uploadDate);
        this.md5 = notNull("md5", md5);
        this.metadata = metadata;
        this.contentType = contentType;
        this.aliases = aliases;
    }

    /**
     * The {@link ObjectId} for this file.
     *
     * @return the id for this file or null if a custom id type has been used.
     */
    public ObjectId getObjectId() {
        if (id.isObjectId()) {
            return id.asObjectId().getValue();
        }
        return null;
    }

    /**
     * The {@link BsonValue} id for this file.
     *
     * @return the id for this file
     */
    public BsonValue getId() {
        return id;
    }

    /**
     * The filename
     *
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * The length, in bytes of this file
     *
     * @return the length, in bytes of this file
     */
    public long getLength() {
        return length;
    }

    /**
     * The size, in bytes, of each data chunk of this file
     *
     * @return the size, in bytes, of each data chunk of this file
     */
    public int getChunkSize() {
        return chunkSize;
    }

    /**
     * The date and time this file was added to GridFS
     *
     * @return the date and time this file was added to GridFS
     */
    public Date getUploadDate() {
        return uploadDate;
    }

    /**
     * The hash of the contents of the stored file
     *
     * @return the hash of the contents of the stored file
     */
    public String getMD5() {
        return md5;
    }

    /**
     * Any additional metadata stored along with the file
     *
     * @return the metadata document
     */
    public Document getMetadata() {
        return metadata;
    }

    /**
     * The content type of the file
     *
     * @return the content type of the file or null
     * @deprecated content type information stored in the metadata document.
     */
    @Deprecated
    public String getContentType() {
        return contentType;
    }

    /**
     * The aliases for the file
     *
     * @return the aliases of the file or null
     * @deprecated aliases should be stored in the metadata document.
     */
    @Deprecated
    public List<String> getAliases() {
        return aliases;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o)  {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        GridFSFile that = (GridFSFile) o;

        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        if (!filename.equals(that.filename)) {
            return false;
        }
        if (length != that.length) {
            return false;
        }
        if (chunkSize != that.chunkSize) {
            return false;
        }
        if (!uploadDate.equals(that.uploadDate)) {
            return false;
        }
        if (!md5.equals(that.md5)) {
            return false;
        }
        if (metadata != null ? !metadata.equals(that.metadata) : that.metadata != null) {
            return false;
        }
        if (contentType != null ? !contentType.equals(that.contentType) : that.contentType != null) {
            return false;
        }
        if (aliases != null ? !aliases.equals(that.aliases) : that.aliases != null) {
            return false;
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + filename.hashCode();
        result = 31 * result + (int) (length ^ (length >>> 32));
        result = 31 * result + chunkSize;
        result = 31 * result + uploadDate.hashCode();
        result = 31 * result + md5.hashCode();
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        result = 31 * result + (contentType != null ? contentType.hashCode() : 0);
        result = 31 * result + (aliases != null ? aliases.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "GridFSFile{"
                + "id=" + id
                + ", filename='" + filename + '\''
                + ", length=" + length
                + ", chunkSize=" + chunkSize
                + ", uploadDate=" + uploadDate
                + ", md5='" + md5 + '\''
                + ", metadata=" + metadata
                + ", contentType='" + contentType + '\''
                + ", aliases=" + aliases
                + '}';
    }
}
