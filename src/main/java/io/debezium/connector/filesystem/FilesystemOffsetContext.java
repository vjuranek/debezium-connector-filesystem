/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;

import io.debezium.connector.SnapshotType;
import io.debezium.pipeline.CommonOffsetContext;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.txmetadata.TransactionContext;
import io.debezium.spi.schema.DataCollectionId;

/**
 * Tracks the connector's position as the last processed cursor (snapshot id / ZFS TXG / Btrfs generation) plus
 * the initial-snapshot progress, so a restart resumes streaming from the next diff without re-emitting data.
 */
public class FilesystemOffsetContext extends CommonOffsetContext<SourceInfo> {

    private final TransactionContext transactionContext;

    public FilesystemOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext) {
        super(sourceInfo);
        this.transactionContext = transactionContext;
    }

    public FilesystemOffsetContext(SourceInfo sourceInfo, TransactionContext transactionContext, boolean snapshotCompleted) {
        super(sourceInfo, snapshotCompleted);
        this.transactionContext = transactionContext;
    }

    public String cursor() {
        return sourceInfo.cursor();
    }

    public void setCursor(String cursor) {
        sourceInfo.setCursor(cursor);
    }

    public SourceInfo sourceInfo() {
        return sourceInfo;
    }

    public void startInitialSnapshot() {
        sourceInfo.startInitialSnapshot();
    }

    @Override
    public Map<String, ?> getOffset() {
        Map<String, Object> offset = new HashMap<>();
        if (sourceInfo.cursor() != null) {
            offset.put(SourceInfo.CURSOR_KEY, sourceInfo.cursor());
        }
        if (getSnapshot().isPresent()) {
            offset.put(SourceInfo.SNAPSHOT_KEY, getSnapshot().get().toString());
            offset.put(SNAPSHOT_COMPLETED_KEY, snapshotCompleted);
        }
        return transactionContext.store(offset);
    }

    @Override
    public Schema getSourceInfoSchema() {
        return sourceInfo.schema();
    }

    @Override
    public void event(DataCollectionId collectionId, Instant timestamp) {
        sourceInfo.event(collectionId != null ? collectionId.identifier() : null, timestamp);
    }

    public void event(String path, Instant timestamp) {
        sourceInfo.event(path, timestamp);
    }

    @Override
    public TransactionContext getTransactionContext() {
        return transactionContext;
    }

    public static FilesystemOffsetContext empty(FilesystemConnectorConfig connectorConfig) {
        return new FilesystemOffsetContext(new SourceInfo(connectorConfig), new TransactionContext());
    }

    @Override
    public String toString() {
        return "FilesystemOffsetContext [sourceInfo=" + sourceInfo + "]";
    }

    public static class Loader implements OffsetContext.Loader<FilesystemOffsetContext> {

        private final FilesystemConnectorConfig connectorConfig;

        public Loader(FilesystemConnectorConfig connectorConfig) {
            this.connectorConfig = connectorConfig;
        }

        @Override
        public FilesystemOffsetContext load(Map<String, ?> offset) {
            final SourceInfo sourceInfo = new SourceInfo(connectorConfig);

            final Object cursor = offset.get(SourceInfo.CURSOR_KEY);
            if (cursor != null) {
                sourceInfo.setCursor(cursor.toString());
            }

            boolean snapshotCompleted = loadSnapshotCompleted(offset);
            final FilesystemOffsetContext offsetContext = new FilesystemOffsetContext(
                    sourceInfo, new TransactionContext(), snapshotCompleted);
            loadSnapshot(offset).ifPresent(snapshotType -> {
                offsetContext.setSnapshot(snapshotType);
                if (snapshotType == SnapshotType.INITIAL && !snapshotCompleted) {
                    sourceInfo.startInitialSnapshot();
                }
            });
            return offsetContext;
        }
    }
}
