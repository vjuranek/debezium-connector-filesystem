/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.btrfs;

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
 * {@link FilesystemSnapshotProvider} backed by Btrfs generation numbers. The cursor is a Btrfs
 * generation; {@code btrfs subvolume find-new <subvol> <gen>} reports files whose extents changed
 * after that generation and the new high-water generation.
 * <p>
 * As documented on {@link BtrfsFindNewParser}, {@code find-new} cannot detect deletions or renames and
 * cannot distinguish create from modify. For complete deletion tracking a {@code btrfs send} stream
 * parser would be required; that is intentionally out of scope for this PoC.
 */
public class BtrfsSnapshotProvider implements FilesystemSnapshotProvider {

    // A generation far beyond any real one, used to cheaply read the current high-water generation:
    // find-new with a future generation returns no files, only the "transid marker was N" line.
    private static final String FUTURE_GENERATION = "9999999999";

    private final Path subvolume;
    private final CommandExecutor executor;
    private final FileMetadataReader metadataReader;

    public BtrfsSnapshotProvider(Path subvolume, boolean checksumEnabled, CommandExecutor executor) {
        this.subvolume = subvolume;
        this.executor = executor;
        this.metadataReader = new FileMetadataReader(checksumEnabled);
    }

    @Override
    public String type() {
        return "btrfs";
    }

    @Override
    public String currentCursor() {
        String output = executor.execute(List.of("btrfs", "subvolume", "find-new", subvolume.toString(), FUTURE_GENERATION));
        String marker = BtrfsFindNewParser.parse(output).transidMarker();
        if (marker == null) {
            throw new DebeziumException("Could not determine current Btrfs generation for subvolume " + subvolume);
        }
        return marker;
    }

    @Override
    public List<FileEntry> listAll(String cursor) {
        try (Stream<Path> files = Files.walk(subvolume)) {
            return files.filter(Files::isRegularFile)
                    .map(metadataReader::toEntry)
                    .toList();
        }
        catch (IOException e) {
            throw new DebeziumException("Failed to walk Btrfs subvolume at " + subvolume, e);
        }
    }

    @Override
    public List<FileChange> diff(String fromCursor, String toCursor) {
        // find-new reports everything changed after fromCursor up to the current generation; toCursor is the
        // high-water mark the caller already resolved via currentCursor() and is used only for offset advancement.
        String output = executor.execute(List.of("btrfs", "subvolume", "find-new", subvolume.toString(), fromCursor));
        return BtrfsFindNewParser.parse(output).changes();
    }
}
