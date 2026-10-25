/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.btrfs;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.filesystem.spi.ChangeType;

/**
 * Unit tests for {@link BtrfsFindNewParser} using recorded {@code btrfs subvolume find-new} output.
 */
public class BtrfsFindNewParserTest {

    @Test
    public void shouldParseChangedFilesAndTransidMarker() {
        String output = String.join("\n",
                "inode 257 file offset 0 len 7 disk start 0 offset 0 gen 8 flags NONE docs/report.txt",
                "inode 258 file offset 0 len 12 disk start 13631488 offset 0 gen 8 flags NONE images/photo.jpg",
                "transid marker was 8");

        BtrfsFindNewParser.Result result = BtrfsFindNewParser.parse(output);

        assertThat(result.transidMarker()).isEqualTo("8");
        assertThat(result.changes()).hasSize(2);
        assertThat(result.changes()).allMatch(c -> c.type() == ChangeType.MODIFIED);
        assertThat(result.changes().get(0).entry().path()).isEqualTo("docs/report.txt");
        assertThat(result.changes().get(1).entry().path()).isEqualTo("images/photo.jpg");
    }

    @Test
    public void shouldReturnOnlyMarkerWhenNoFilesChanged() {
        String output = "transid marker was 42";

        BtrfsFindNewParser.Result result = BtrfsFindNewParser.parse(output);

        assertThat(result.transidMarker()).isEqualTo("42");
        assertThat(result.changes()).isEmpty();
    }

    @Test
    public void shouldHandleEmptyOutput() {
        BtrfsFindNewParser.Result result = BtrfsFindNewParser.parse("");

        assertThat(result.transidMarker()).isNull();
        assertThat(result.changes()).isEmpty();
    }

    @Test
    public void shouldExtractPathWithInlineFlag() {
        String output = "inode 260 file offset 0 len 5 disk start 0 offset 0 gen 10 flags INLINE notes/small.txt";

        BtrfsFindNewParser.Result result = BtrfsFindNewParser.parse(output);

        assertThat(result.changes()).hasSize(1);
        assertThat(result.changes().get(0).entry().path()).isEqualTo("notes/small.txt");
    }
}
