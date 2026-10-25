/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.btrfs;

import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.connector.filesystem.spi.FileEntry;

/**
 * Parses the output of {@code btrfs subvolume find-new <subvolume> <generation>}.
 * <p>
 * Each data line looks like:
 * <pre>
 * inode 257 file offset 0 len 7 disk start 0 offset 0 gen 8 flags NONE path/to/file
 * </pre>
 * and the final line reports the new high-water generation:
 * <pre>
 * transid marker was 8
 * </pre>
 * <p>
 * Note the inherent limitations of {@code find-new}: it reports files whose extents changed after the
 * given generation but cannot distinguish a create from a modify, and it does <em>not</em> report
 * deletions or renames. The connector therefore maps every reported file to a
 * {@link io.debezium.connector.filesystem.spi.ChangeType#MODIFIED MODIFIED} (upsert) change. Detecting
 * deletions requires parsing a {@code btrfs send} stream, which is out of scope for this PoC.
 */
public final class BtrfsFindNewParser {

    private static final String TRANSID_PREFIX = "transid marker was ";
    private static final String FLAGS_TOKEN = " flags ";

    private BtrfsFindNewParser() {
    }

    /**
     * @param changes the files reported as created/modified
     * @param transidMarker the new generation cursor reported by {@code find-new}, or {@code null} if absent
     */
    public record Result(List<FileChange> changes, String transidMarker) {
    }

    public static Result parse(String output) {
        List<FileChange> changes = new ArrayList<>();
        String transidMarker = null;

        if (output == null || output.isBlank()) {
            return new Result(changes, null);
        }

        for (String line : output.split("\n")) {
            String trimmed = line.strip();
            if (trimmed.isEmpty()) {
                continue;
            }

            if (trimmed.startsWith(TRANSID_PREFIX)) {
                transidMarker = trimmed.substring(TRANSID_PREFIX.length()).trim();
                continue;
            }

            if (!trimmed.startsWith("inode ")) {
                continue;
            }

            String path = extractPath(trimmed);
            if (path != null && !path.isBlank()) {
                changes.add(FileChange.modified(FileEntry.ofPath(path)));
            }
        }

        return new Result(changes, transidMarker);
    }

    /**
     * The path is the remainder of the line after the {@code flags <FLAGS>} token.
     */
    private static String extractPath(String line) {
        int flagsIdx = line.indexOf(FLAGS_TOKEN);
        if (flagsIdx < 0) {
            return null;
        }
        // Skip past " flags " and the single flags value token that follows.
        int afterFlags = flagsIdx + FLAGS_TOKEN.length();
        int flagsValueEnd = line.indexOf(' ', afterFlags);
        if (flagsValueEnd < 0) {
            return null;
        }
        return line.substring(flagsValueEnd + 1).trim();
    }
}
