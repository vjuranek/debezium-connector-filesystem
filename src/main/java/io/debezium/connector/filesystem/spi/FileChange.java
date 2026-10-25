/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

/**
 * A single file-level change detected by diffing two filesystem snapshots.
 *
 * @param type the kind of change
 * @param entry the (current) metadata of the affected file; for {@link ChangeType#DELETED} this may only carry the path
 * @param previousPath the former path for a {@link ChangeType#RENAMED} change, otherwise {@code null}
 */
public record FileChange(ChangeType type, FileEntry entry, String previousPath) {

    public FileChange {
        if (type == null) {
            throw new IllegalArgumentException("Change type must not be null");
        }
        if (entry == null) {
            throw new IllegalArgumentException("File entry must not be null");
        }
        if (type == ChangeType.RENAMED && (previousPath == null || previousPath.isBlank())) {
            throw new IllegalArgumentException("A RENAMED change requires the previous path");
        }
    }

    public static FileChange created(FileEntry entry) {
        return new FileChange(ChangeType.CREATED, entry, null);
    }

    public static FileChange modified(FileEntry entry) {
        return new FileChange(ChangeType.MODIFIED, entry, null);
    }

    public static FileChange deleted(FileEntry entry) {
        return new FileChange(ChangeType.DELETED, entry, null);
    }

    public static FileChange renamed(FileEntry entry, String previousPath) {
        return new FileChange(ChangeType.RENAMED, entry, previousPath);
    }
}
