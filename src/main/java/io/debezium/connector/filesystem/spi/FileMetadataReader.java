/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

import io.debezium.DebeziumException;

/**
 * Reads filesystem metadata for a {@link Path} into a {@link FileEntry}. Used by provider implementations
 * during the initial snapshot walk. Kept separate from the snapshot providers so the diff parsers stay
 * free of any real-filesystem dependency.
 */
public final class FileMetadataReader {

    private final boolean checksumEnabled;

    public FileMetadataReader(boolean checksumEnabled) {
        this.checksumEnabled = checksumEnabled;
    }

    public FileEntry toEntry(Path file) {
        try {
            long size = Files.size(file);
            long mtime = Files.getLastModifiedTime(file).toMillis();
            String mode = null;
            String owner = null;
            try {
                PosixFileAttributes posix = Files.readAttributes(file, PosixFileAttributes.class);
                mode = PosixFilePermissions.toString(posix.permissions());
                owner = posix.owner().getName();
            }
            catch (UnsupportedOperationException | IOException e) {
                // POSIX attributes unavailable on this filesystem; metadata remains null.
            }
            String checksum = checksumEnabled ? sha256(file) : null;
            return new FileEntry(file.toString(), size, mtime, mode, owner, checksum);
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to read metadata for " + file, e);
        }
    }

    private static String sha256(Path file) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buffer = new byte[8192];
            try (InputStream in = Files.newInputStream(file)) {
                int read;
                while ((read = in.read(buffer)) != -1) {
                    digest.update(buffer, 0, read);
                }
            }
            return HexFormat.of().formatHex(digest.digest());
        }
        catch (NoSuchAlgorithmException | IOException e) {
            throw new DebeziumException("Failed to compute checksum for " + file, e);
        }
    }
}
