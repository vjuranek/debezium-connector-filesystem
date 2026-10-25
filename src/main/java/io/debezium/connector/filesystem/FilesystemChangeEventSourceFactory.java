/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.source.spi.ChangeEventSourceFactory;
import io.debezium.pipeline.source.spi.SnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.util.Clock;

/**
 * Wires the snapshot and streaming phases for the filesystem connector. Incremental snapshots are out of scope for
 * the PoC, so the default (empty) incremental source from the interface is used.
 */
public class FilesystemChangeEventSourceFactory implements ChangeEventSourceFactory<FilesystemPartition, FilesystemOffsetContext> {

    private final FilesystemConnectorConfig connectorConfig;
    private final EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher;
    private final Clock clock;
    private final SnapshotterService snapshotterService;

    public FilesystemChangeEventSourceFactory(FilesystemConnectorConfig connectorConfig,
                                              EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher,
                                              Clock clock,
                                              SnapshotterService snapshotterService) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshotChangeEventSource<FilesystemPartition, FilesystemOffsetContext> getSnapshotChangeEventSource(
                                                                                                                SnapshotProgressListener<FilesystemPartition> snapshotProgressListener,
                                                                                                                NotificationService<FilesystemPartition, FilesystemOffsetContext> notificationService) {
        return new FilesystemSnapshotChangeEventSource(connectorConfig, dispatcher, clock, snapshotProgressListener, notificationService, snapshotterService);
    }

    @Override
    public StreamingChangeEventSource<FilesystemPartition, FilesystemOffsetContext> getStreamingChangeEventSource() {
        return new FilesystemStreamingChangeEventSource(connectorConfig, dispatcher, clock);
    }
}
