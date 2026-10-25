/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.zfs;

import java.util.ArrayList;
import java.util.List;

import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.connector.filesystem.spi.FileEntry;

/**
 * Parses the output of {@code zfs diff <snapshotA> <snapshotB>}.
 * <p>
 * Each line is tab-separated as {@code <change>\t<path>}, where {@code <change>} is one of:
 * <ul>
 *   <li>{@code +} &mdash; the path was added (CREATE)</li>
 *   <li>{@code -} &mdash; the path was removed (DELETE)</li>
 *   <li>{@code M} &mdash; the path was modified (UPDATE)</li>
 *   <li>{@code R} &mdash; the path was renamed; the line is {@code R\t<oldPath> -> <newPath>}</li>
 * </ul>
 * Directory entries (paths ending in {@code /}) are skipped; the connector reports files only.
 */
public final class ZfsDiffParser {

    private static final String RENAME_SEPARATOR = " -> ";

    private ZfsDiffParser() {
    }

    public static List<FileChange> parse(String output) {
        List<FileChange> changes = new ArrayList<>();
        if (output == null || output.isBlank()) {
            return changes;
        }

        for (String line : output.split("\n")) {
            if (line.isBlank()) {
                continue;
            }

            int tab = line.indexOf('\t');
            if (tab < 0) {
                continue;
            }

            String change = line.substring(0, tab).trim();
            String remainder = line.substring(tab + 1).trim();
            if (remainder.isEmpty()) {
                continue;
            }

            switch (change) {
                case "+" -> addFileChange(changes, FileChange.created(FileEntry.ofPath(remainder)), remainder);
                case "-" -> addFileChange(changes, FileChange.deleted(FileEntry.ofPath(remainder)), remainder);
                case "M" -> addFileChange(changes, FileChange.modified(FileEntry.ofPath(remainder)), remainder);
                case "R" -> {
                    int sep = remainder.indexOf(RENAME_SEPARATOR);
                    if (sep > 0) {
                        String oldPath = remainder.substring(0, sep).trim();
                        String newPath = remainder.substring(sep + RENAME_SEPARATOR.length()).trim();
                        if (!newPath.endsWith("/")) {
                            changes.add(FileChange.renamed(FileEntry.ofPath(newPath), oldPath));
                        }
                    }
                }
                default -> {
                    // Unknown change marker; ignore defensively.
                }
            }
        }
        return changes;
    }

    private static void addFileChange(List<FileChange> changes, FileChange change, String path) {
        if (!path.endsWith("/")) {
            changes.add(change);
        }
    }
}
