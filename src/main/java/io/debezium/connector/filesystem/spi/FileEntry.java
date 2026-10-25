/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

/**
 * Metadata describing a single file at a point in time. This is intentionally metadata-only: the file
 * content never enters the change event. Downstream consumers use {@link #path()} as a pointer to fetch
 * and process the file out-of-band (for example an AI/OCR/embedding pipeline).
 *
 * @param path the absolute path of the file within the watched root
 * @param size the file size in bytes, or {@code -1} if unknown (for example for deletions)
 * @param modificationTimeMs the last-modification time in epoch milliseconds, or {@code -1} if unknown
 * @param mode the POSIX permission bits rendered as an octal string (for example {@code "0644"}), or {@code null} if unknown
 * @param owner the owning user name or uid, or {@code null} if unknown
 * @param checksum an optional content checksum (for example SHA-256 hex), or {@code null} when checksums are disabled or the file is gone
 */
public record FileEntry(
        String path,
        long size,
        long modificationTimeMs,
        String mode,
        String owner,
        String checksum) {

    public static final long UNKNOWN = -1L;

    public FileEntry {
        if (path == null || path.isBlank()) {
            throw new IllegalArgumentException("File path must not be null or blank");
        }
    }

    /**
     * Creates a metadata-only entry for a path whose attributes are not available (for example a deleted file).
     */
    public static FileEntry ofPath(String path) {
        return new FileEntry(path, UNKNOWN, UNKNOWN, null, null, null);
    }
}
