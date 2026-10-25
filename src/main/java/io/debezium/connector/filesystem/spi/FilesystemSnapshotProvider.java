/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

import java.util.List;

/**
 * Pluggable backend that exposes a copy-on-write filesystem's snapshot machinery as a change feed.
 * <p>
 * The abstraction mirrors log-based CDC: a <em>cursor</em> is an opaque, monotonically advancing position
 * (a ZFS birth TXG, a Btrfs generation number, or a snapshot id) that plays the role of an LSN. The
 * connector persists the last processed cursor as its offset and resumes from it after a restart.
 * <p>
 * Implementations are expected to be cheap to call and to shell out to the native tooling
 * ({@code zfs diff}, {@code btrfs subvolume find-new}, ...) behind a {@link CommandExecutor} so the
 * parsing logic can be unit-tested without a real filesystem.
 */
public interface FilesystemSnapshotProvider extends AutoCloseable {

    /**
     * @return a short identifier for this backend (for example {@code "btrfs"} or {@code "zfs"}); surfaced
     *         in the event {@code source} block.
     */
    String type();

    /**
     * Returns the most recent cursor available on the watched root. This is the high-water mark the
     * streaming phase diffs towards.
     *
     * @return the current cursor, never {@code null}
     */
    String currentCursor();

    /**
     * Lists every file visible at the given cursor. Used by the initial snapshot phase to emit a
     * {@link ChangeType#READ} baseline.
     *
     * @param cursor the cursor to enumerate; typically {@link #currentCursor()}
     * @return the files present at that cursor
     */
    List<FileEntry> listAll(String cursor);

    /**
     * Diffs two cursors and returns the file-level changes between them. Because snapshots are periodic,
     * changes are coalesced: a file created and deleted within the interval is not reported, and only the
     * net effect of multiple modifications is visible.
     *
     * @param fromCursor the previously processed cursor (exclusive)
     * @param toCursor the target cursor (inclusive)
     * @return the ordered list of changes, empty when nothing changed
     */
    List<FileChange> diff(String fromCursor, String toCursor);

    @Override
    default void close() {
    }
}
