/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.filesystem.spi.ChangeType;
import io.debezium.connector.filesystem.spi.FileChange;
import io.debezium.data.Envelope.Operation;
import io.debezium.pipeline.AbstractChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.util.Clock;

/**
 * Builds metadata-only change records from a single {@link FileChange}. There is no before-image available from the
 * filesystem, so {@code before} is populated only for deletes (with the path that disappeared).
 */
public class FilesystemChangeRecordEmitter extends AbstractChangeRecordEmitter<FilesystemPartition, FilesystemCollectionSchema> {

    private final FileChange change;
    private final Operation operation;

    public FilesystemChangeRecordEmitter(FilesystemPartition partition, OffsetContext offsetContext, Clock clock,
                                         FilesystemConnectorConfig connectorConfig, FileChange change) {
        super(partition, offsetContext, clock, connectorConfig);
        this.change = change;
        this.operation = toOperation(change.type());
    }

    /**
     * Maps a filesystem {@link ChangeType} to a Debezium {@link Operation}. Renames are surfaced as a create of the
     * new path (the PoC does not emit a paired delete of the old path).
     */
    public static Operation toOperation(ChangeType type) {
        return switch (type) {
            case READ -> Operation.READ;
            case CREATED, RENAMED -> Operation.CREATE;
            case MODIFIED -> Operation.UPDATE;
            case DELETED -> Operation.DELETE;
        };
    }

    @Override
    public Operation getOperation() {
        return operation;
    }

    private Struct sourceStruct() {
        return getOffset().getSourceInfo();
    }

    private java.time.Instant eventTime() {
        return getClock().currentTimeAsInstant();
    }

    @Override
    protected void emitReadRecord(Receiver<FilesystemPartition> receiver, FilesystemCollectionSchema schema) throws InterruptedException {
        final Struct after = schema.metadataFromEntry(change.entry());
        final Struct envelope = schema.getEnvelopeSchema().read(after, sourceStruct(), eventTime());
        receiver.changeRecord(getPartition(), schema, Operation.READ, schema.keyFromPath(change.entry().path()), envelope, getOffset(), null);
    }

    @Override
    protected void emitCreateRecord(Receiver<FilesystemPartition> receiver, FilesystemCollectionSchema schema) throws InterruptedException {
        final Struct after = schema.metadataFromEntry(change.entry());
        final Struct envelope = schema.getEnvelopeSchema().create(after, sourceStruct(), eventTime());
        receiver.changeRecord(getPartition(), schema, Operation.CREATE, schema.keyFromPath(change.entry().path()), envelope, getOffset(), null);
    }

    @Override
    protected void emitUpdateRecord(Receiver<FilesystemPartition> receiver, FilesystemCollectionSchema schema) throws InterruptedException {
        final Struct after = schema.metadataFromEntry(change.entry());
        final Struct envelope = schema.getEnvelopeSchema().update(null, after, sourceStruct(), eventTime());
        receiver.changeRecord(getPartition(), schema, Operation.UPDATE, schema.keyFromPath(change.entry().path()), envelope, getOffset(), null);
    }

    @Override
    protected void emitDeleteRecord(Receiver<FilesystemPartition> receiver, FilesystemCollectionSchema schema) throws InterruptedException {
        final Struct before = schema.metadataFromEntry(change.entry());
        final Struct envelope = schema.getEnvelopeSchema().delete(before, sourceStruct(), eventTime());
        receiver.changeRecord(getPartition(), schema, Operation.DELETE, schema.keyFromPath(change.entry().path()), envelope, getOffset(), null);
    }
}
