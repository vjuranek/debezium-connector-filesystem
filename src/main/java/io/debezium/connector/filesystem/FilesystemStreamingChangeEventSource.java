/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.List;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.connector.filesystem.spi.FilesystemSnapshotProvider;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.StreamingChangeEventSource;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;

/**
 * The "streaming" phase, which for a snapshot-diffing filesystem is really a batch pulse: on each poll interval it
 * asks the backend for the newest cursor (ZFS TXG / Btrfs generation / snapshot id), diffs it against the last
 * processed cursor, and dispatches one {@code CREATE}/{@code UPDATE}/{@code DELETE} per reported {@link FileChange}.
 */
public class FilesystemStreamingChangeEventSource implements StreamingChangeEventSource<FilesystemPartition, FilesystemOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemStreamingChangeEventSource.class);

    private final FilesystemConnectorConfig connectorConfig;
    private final EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher;
    private final Clock clock;

    public FilesystemStreamingChangeEventSource(FilesystemConnectorConfig connectorConfig,
                                                EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher,
                                                Clock clock) {
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.clock = clock;
    }

    @Override
    public void execute(ChangeEventSourceContext context, FilesystemPartition partition, FilesystemOffsetContext offsetContext)
            throws InterruptedException {
        if (offsetContext == null) {
            offsetContext = FilesystemOffsetContext.empty(connectorConfig);
        }
        final FilesystemCollectionId collectionId = connectorConfig.getCollectionId();
        final Metronome metronome = Metronome.sleeper(connectorConfig.getSnapshotPollInterval(), clock);

        try (FilesystemSnapshotProvider provider = connectorConfig.createSnapshotProvider()) {
            String lastCursor = offsetContext.cursor();
            if (lastCursor == null) {
                // No snapshot phase ran (e.g. snapshot.mode=no_data): start diffing from the current cursor.
                lastCursor = provider.currentCursor();
                offsetContext.setCursor(lastCursor);
                LOGGER.info("No stored cursor; streaming diffs from current cursor '{}'", lastCursor);
            }
            else {
                LOGGER.info("Streaming diffs starting from stored cursor '{}'", lastCursor);
            }

            while (context.isRunning()) {
                metronome.pause();
                if (!context.isRunning()) {
                    break;
                }

                final String newCursor = provider.currentCursor();
                if (Objects.equals(newCursor, lastCursor)) {
                    dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
                    continue;
                }

                LOGGER.info("Diffing cursor '{}' -> '{}'", lastCursor, newCursor);
                final List<FileChange> changes = provider.diff(lastCursor, newCursor);
                for (FileChange change : changes) {
                    if (!context.isRunning()) {
                        break;
                    }
                    final String path = change.entry().path();
                    offsetContext.event(path, clock.currentTimeAsInstant());
                    dispatcher.dispatchDataChangeEvent(partition, collectionId,
                            new FilesystemChangeRecordEmitter(partition, offsetContext, clock, connectorConfig, change));
                }

                offsetContext.setCursor(newCursor);
                lastCursor = newCursor;
                LOGGER.info("Advanced cursor to '{}' after dispatching {} change(s)", newCursor, changes.size());
                dispatcher.dispatchHeartbeatEvent(partition, offsetContext);
            }
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw e;
        }
        catch (Exception e) {
            throw new DebeziumException("Filesystem streaming failed", e);
        }
    }
}
