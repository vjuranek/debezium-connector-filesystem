/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.filesystem.spi.ChangeType;
import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.connector.filesystem.spi.FileEntry;
import io.debezium.connector.filesystem.spi.FilesystemSnapshotProvider;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.EventDispatcher.SnapshotReceiver;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.actions.snapshotting.SnapshotConfiguration;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.SnapshottingTask;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.snapshot.Snapshotter;
import io.debezium.util.Clock;

/**
 * The initial-snapshot phase: walk the current tree reported by the {@link FilesystemSnapshotProvider} and emit a
 * {@code READ} event per file, recording the snapshot cursor into the offset so streaming resumes from there.
 */
public class FilesystemSnapshotChangeEventSource extends AbstractSnapshotChangeEventSource<FilesystemPartition, FilesystemOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemSnapshotChangeEventSource.class);

    private final FilesystemConnectorConfig connectorConfig;
    private final EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher;
    private final Clock clock;
    private final SnapshotterService snapshotterService;

    public FilesystemSnapshotChangeEventSource(FilesystemConnectorConfig connectorConfig,
                                               EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher,
                                               Clock clock,
                                               SnapshotProgressListener<FilesystemPartition> snapshotProgressListener,
                                               NotificationService<FilesystemPartition, FilesystemOffsetContext> notificationService,
                                               SnapshotterService snapshotterService) {
        super(connectorConfig, snapshotProgressListener, notificationService);
        this.connectorConfig = connectorConfig;
        this.dispatcher = dispatcher;
        this.clock = clock;
        this.snapshotterService = snapshotterService;
    }

    @Override
    public SnapshottingTask getSnapshottingTask(FilesystemPartition partition, FilesystemOffsetContext previousOffset) {
        final Snapshotter snapshotter = snapshotterService.getSnapshotter();
        final boolean offsetExists = previousOffset != null;
        final boolean snapshotInProgress = offsetExists && previousOffset.isInitialSnapshotRunning();

        final boolean shouldSnapshotSchema = snapshotter.shouldSnapshotSchema(offsetExists, snapshotInProgress);
        final boolean shouldSnapshotData = snapshotter.shouldSnapshotData(offsetExists, snapshotInProgress);

        if (!shouldSnapshotData) {
            LOGGER.info("According to the connector configuration, no initial tree walk will be performed");
        }
        return new SnapshottingTask(shouldSnapshotSchema, shouldSnapshotData, List.of(), Map.of(), false);
    }

    @Override
    public SnapshottingTask getBlockingSnapshottingTask(FilesystemPartition partition, FilesystemOffsetContext previousOffset,
                                                        SnapshotConfiguration snapshotConfiguration) {
        // Blocking (on-demand) snapshots are out of scope for the PoC: always re-walk the data for the requested collections.
        return new SnapshottingTask(false, true, snapshotConfiguration.getDataCollections(), Map.of(), true);
    }

    @Override
    protected SnapshotContext<FilesystemPartition, FilesystemOffsetContext> prepare(FilesystemPartition partition, boolean onDemand) {
        return new SnapshotContext<>(partition);
    }

    @Override
    protected SnapshotResult<FilesystemOffsetContext> doExecute(ChangeEventSourceContext context,
                                                                FilesystemOffsetContext previousOffset,
                                                                SnapshotContext<FilesystemPartition, FilesystemOffsetContext> snapshotContext,
                                                                SnapshottingTask snapshottingTask)
            throws Exception {
        final FilesystemCollectionId collectionId = connectorConfig.getCollectionId();

        try (FilesystemSnapshotProvider provider = connectorConfig.createSnapshotProvider()) {
            final FilesystemOffsetContext offset = FilesystemOffsetContext.empty(connectorConfig);
            snapshotContext.offset = offset;

            final String cursor = provider.currentCursor();
            LOGGER.info("Starting initial tree walk of '{}' at cursor '{}'", connectorConfig.getFilesystemPath(), cursor);

            offset.preSnapshotStart(snapshottingTask.isOnDemand());
            offset.startInitialSnapshot();
            offset.setCursor(cursor);

            final SnapshotReceiver<FilesystemPartition> receiver = dispatcher.getSnapshotChangeEventReceiver();

            final List<FileEntry> files = provider.listAll(cursor);
            long count = 0;
            for (FileEntry file : files) {
                if (!context.isRunning()) {
                    throw new InterruptedException("Interrupted while performing initial tree walk");
                }
                offset.event(file.path(), clock.currentTimeAsInstant());
                final FileChange change = new FileChange(ChangeType.READ, file, null);
                dispatcher.dispatchSnapshotEvent(snapshotContext.partition, collectionId,
                        new FilesystemChangeRecordEmitter(snapshotContext.partition, offset, clock, connectorConfig, change),
                        receiver);
                count++;
            }

            offset.preSnapshotCompletion();
            receiver.completeSnapshot();
            offset.postSnapshotCompletion();

            LOGGER.info("Initial tree walk completed; emitted {} file(s) at cursor '{}'", count, cursor);
            return SnapshotResult.completed(offset);
        }
        catch (InterruptedException e) {
            throw e;
        }
        catch (Exception e) {
            throw new DebeziumException("Initial tree walk failed", e);
        }
    }
}
