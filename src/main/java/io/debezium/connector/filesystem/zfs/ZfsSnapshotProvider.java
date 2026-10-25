/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.zfs;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

import io.debezium.DebeziumException;
import io.debezium.connector.filesystem.spi.CommandExecutor;
import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.connector.filesystem.spi.FileEntry;
import io.debezium.connector.filesystem.spi.FileMetadataReader;
import io.debezium.connector.filesystem.spi.FilesystemSnapshotProvider;

/**
 * {@link FilesystemSnapshotProvider} backed by ZFS snapshots. The cursor is a snapshot name
 * ({@code dataset@snapshot}); {@code zfs diff} between two snapshot cursors yields the change feed.
 */
public class ZfsSnapshotProvider implements FilesystemSnapshotProvider {

    private final String dataset;
    private final Path mountRoot;
    private final CommandExecutor executor;
    private final FileMetadataReader metadataReader;

    public ZfsSnapshotProvider(String dataset, Path mountRoot, boolean checksumEnabled, CommandExecutor executor) {
        this.dataset = dataset;
        this.mountRoot = mountRoot;
        this.executor = executor;
        this.metadataReader = new FileMetadataReader(checksumEnabled);
    }

    @Override
    public String type() {
        return "zfs";
    }

    @Override
    public String currentCursor() {
        String output = executor.execute(List.of("zfs", "list", "-t", "snapshot", "-H", "-o", "name", "-s", "creation", "-r", dataset));
        String last = null;
        for (String line : output.split("\n")) {
            if (!line.isBlank()) {
                last = line.trim();
            }
        }
        if (last == null) {
            throw new DebeziumException("No ZFS snapshots found for dataset " + dataset
                    + "; create an initial snapshot (zfs snapshot " + dataset + "@debezium-init) before starting the connector");
        }
        return last;
    }

    @Override
    public List<FileEntry> listAll(String cursor) {
        // The snapshot is browsable under <mountRoot>/.zfs/snapshot/<snapshotName>.
        String snapshotName = cursor.contains("@") ? cursor.substring(cursor.indexOf('@') + 1) : cursor;
        Path snapshotRoot = mountRoot.resolve(".zfs").resolve("snapshot").resolve(snapshotName);
        Path walkRoot = Files.isDirectory(snapshotRoot) ? snapshotRoot : mountRoot;
        try (Stream<Path> files = Files.walk(walkRoot)) {
            return files.filter(Files::isRegularFile)
                    .map(metadataReader::toEntry)
                    .toList();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to walk ZFS snapshot at " + walkRoot, e);
        }
    }

    @Override
    public List<FileChange> diff(String fromCursor, String toCursor) {
        String output = executor.execute(List.of("zfs", "diff", fromCursor, toCursor));
        return ZfsDiffParser.parse(output);
    }
}
