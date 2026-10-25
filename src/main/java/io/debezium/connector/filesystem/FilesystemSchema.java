/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.schema.DatabaseSchema;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.spi.topic.TopicNamingStrategy;

/**
 * Holds the fixed, metadata-only schema for the filesystem connector. There is no historized schema to recover:
 * every collection shares the same key/value shape, lazily materialized per collection id.
 */
@ThreadSafe
public class FilesystemSchema implements DatabaseSchema<FilesystemCollectionId> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemSchema.class);

    private final TopicNamingStrategy<FilesystemCollectionId> topicNamingStrategy;
    private final Schema sourceSchema;
    private final SchemaNameAdjuster adjuster;
    private final ConcurrentMap<FilesystemCollectionId, FilesystemCollectionSchema> collections = new ConcurrentHashMap<>();

    public FilesystemSchema(TopicNamingStrategy<FilesystemCollectionId> topicNamingStrategy, Schema sourceSchema, SchemaNameAdjuster adjuster) {
        this.topicNamingStrategy = topicNamingStrategy;
        this.sourceSchema = sourceSchema;
        this.adjuster = adjuster;
    }

    @Override
    public void close() {
    }

    @Override
    public DataCollectionSchema schemaFor(FilesystemCollectionId collectionId) {
        return collections.computeIfAbsent(collectionId,
                id -> new FilesystemCollectionSchema(id, topicNamingStrategy.dataChangeTopic(id), sourceSchema, adjuster));
    }

    @Override
    public boolean tableInformationComplete() {
        return false;
    }

    @Override
    public void assureNonEmptySchema() {
        if (collections.isEmpty()) {
            LOGGER.warn(DatabaseSchema.NO_CAPTURED_DATA_COLLECTIONS_WARNING);
        }
    }

    @Override
    public boolean isHistorized() {
        return false;
    }
}
