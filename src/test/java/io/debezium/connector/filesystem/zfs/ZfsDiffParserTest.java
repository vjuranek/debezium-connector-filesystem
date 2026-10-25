/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.zfs;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

import io.debezium.connector.filesystem.spi.ChangeType;
import io.debezium.connector.filesystem.spi.FileChange;

/**
 * Unit tests for {@link ZfsDiffParser} using recorded {@code zfs diff} output; no real filesystem required.
 */
public class ZfsDiffParserTest {

    @Test
    public void shouldReturnEmptyForBlankOutput() {
        assertThat(ZfsDiffParser.parse(null)).isEmpty();
        assertThat(ZfsDiffParser.parse("")).isEmpty();
        assertThat(ZfsDiffParser.parse("   \n  ")).isEmpty();
    }

    @Test
    public void shouldParseAddedModifiedAndRemovedFiles() {
        String output = String.join("\n",
                "+\t/pool/fs/newfile.txt",
                "M\t/pool/fs/changed.txt",
                "-\t/pool/fs/gone.txt");

        List<FileChange> changes = ZfsDiffParser.parse(output);

        assertThat(changes).hasSize(3);
        assertThat(changes.get(0).type()).isEqualTo(ChangeType.CREATED);
        assertThat(changes.get(0).entry().path()).isEqualTo("/pool/fs/newfile.txt");
        assertThat(changes.get(1).type()).isEqualTo(ChangeType.MODIFIED);
        assertThat(changes.get(1).entry().path()).isEqualTo("/pool/fs/changed.txt");
        assertThat(changes.get(2).type()).isEqualTo(ChangeType.DELETED);
        assertThat(changes.get(2).entry().path()).isEqualTo("/pool/fs/gone.txt");
    }

    @Test
    public void shouldParseRenameWithPreviousPath() {
        String output = "R\t/pool/fs/old-name.txt -> /pool/fs/new-name.txt";

        List<FileChange> changes = ZfsDiffParser.parse(output);

        assertThat(changes).hasSize(1);
        FileChange rename = changes.get(0);
        assertThat(rename.type()).isEqualTo(ChangeType.RENAMED);
        assertThat(rename.entry().path()).isEqualTo("/pool/fs/new-name.txt");
        assertThat(rename.previousPath()).isEqualTo("/pool/fs/old-name.txt");
    }

    @Test
    public void shouldSkipDirectoryEntries() {
        String output = String.join("\n",
                "M\t/pool/fs/",
                "M\t/pool/fs/subdir/",
                "+\t/pool/fs/subdir/file.txt");

        List<FileChange> changes = ZfsDiffParser.parse(output);

        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).entry().path()).isEqualTo("/pool/fs/subdir/file.txt");
    }

    @Test
    public void shouldIgnoreMalformedLines() {
        String output = String.join("\n",
                "this line has no tab",
                "?\t/pool/fs/unknown-marker.txt",
                "+\t/pool/fs/good.txt");

        List<FileChange> changes = ZfsDiffParser.parse(output);

        assertThat(changes).hasSize(1);
        assertThat(changes.get(0).entry().path()).isEqualTo("/pool/fs/good.txt");
    }
}
