/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;

import org.junit.jupiter.api.Test;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;

/**
 * Unit tests for {@link FilesystemOffsetContext} persistence: a stored cursor and snapshot state must round-trip
 * through {@link FilesystemOffsetContext.Loader} so a restart resumes from the right place.
 */
public class FilesystemOffsetContextTest {

    private static FilesystemConnectorConfig newConfig() {
        final Configuration config = Configuration.create()
                .with(FilesystemConnectorConfig.FILESYSTEM_TYPE, FilesystemConnectorConfig.TYPE_BTRFS)
                .with(FilesystemConnectorConfig.FILESYSTEM_PATH, "/data/subvol")
                .with(CommonConnectorConfig.TOPIC_PREFIX, "fs1")
                .build();
        return new FilesystemConnectorConfig(config);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRoundTripCursorWhileSnapshotRunning() {
        final FilesystemConnectorConfig connectorConfig = newConfig();

        final FilesystemOffsetContext offset = FilesystemOffsetContext.empty(connectorConfig);
        offset.preSnapshotStart(false);
        offset.startInitialSnapshot();
        offset.setCursor("100");

        final Map<String, ?> stored = offset.getOffset();
        assertThat(stored.get(SourceInfo.CURSOR_KEY)).isEqualTo("100");

        final FilesystemOffsetContext loaded = new FilesystemOffsetContext.Loader(connectorConfig).load((Map<String, Object>) stored);
        assertThat(loaded.cursor()).isEqualTo("100");
        assertThat(loaded.isInitialSnapshotRunning()).isTrue();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRoundTripCursorAfterSnapshotCompleted() {
        final FilesystemConnectorConfig connectorConfig = newConfig();

        final FilesystemOffsetContext offset = FilesystemOffsetContext.empty(connectorConfig);
        offset.preSnapshotStart(false);
        offset.setCursor("100");
        offset.preSnapshotCompletion();
        offset.postSnapshotCompletion();
        offset.setCursor("200");

        final Map<String, ?> stored = offset.getOffset();
        final FilesystemOffsetContext loaded = new FilesystemOffsetContext.Loader(connectorConfig).load((Map<String, Object>) stored);

        assertThat(loaded.cursor()).isEqualTo("200");
        assertThat(loaded.isInitialSnapshotRunning()).isFalse();
    }
}
