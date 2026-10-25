/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.filesystem.spi.FileEntry;
import io.debezium.data.Envelope;
import io.debezium.data.SchemaUtil;
import io.debezium.schema.DataCollectionSchema;
import io.debezium.spi.schema.DataCollectionId;

/**
 * The Kafka Connect schema for a filesystem collection: a key keyed by file path and a metadata-only value
 * ({@code path, size, mtime, mode, owner, checksum}) wrapped in the standard Debezium envelope.
 */
public class FilesystemCollectionSchema implements DataCollectionSchema {

    public static final String PATH = "path";
    public static final String SIZE = "size";
    public static final String MTIME = "mtime";
    public static final String MODE = "mode";
    public static final String OWNER = "owner";
    public static final String CHECKSUM = "checksum";

    private final FilesystemCollectionId id;
    private final Schema keySchema;
    private final Schema metadataSchema;
    private final Schema valueSchema;
    private final Envelope envelopeSchema;

    public FilesystemCollectionSchema(FilesystemCollectionId id, String topicName, Schema sourceSchema, io.debezium.schema.SchemaNameAdjuster adjuster) {
        this.id = id;
        this.keySchema = SchemaBuilder.struct()
                .name(adjuster.adjust(topicName + ".Key"))
                .field(PATH, Schema.STRING_SCHEMA)
                .build();
        this.metadataSchema = SchemaBuilder.struct()
                .name(adjuster.adjust(topicName + ".Value"))
                .field(PATH, Schema.STRING_SCHEMA)
                .field(SIZE, Schema.OPTIONAL_INT64_SCHEMA)
                .field(MTIME, Schema.OPTIONAL_INT64_SCHEMA)
                .field(MODE, Schema.OPTIONAL_STRING_SCHEMA)
                .field(OWNER, Schema.OPTIONAL_STRING_SCHEMA)
                .field(CHECKSUM, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
        this.envelopeSchema = Envelope.defineSchema()
                .withName(adjuster.adjust(Envelope.schemaName(topicName)))
                .withRecord(metadataSchema)
                .withSource(sourceSchema)
                .build();
        this.valueSchema = envelopeSchema.schema();
    }

    @Override
    public DataCollectionId id() {
        return id;
    }

    @Override
    public Schema keySchema() {
        return keySchema;
    }

    public Schema valueSchema() {
        return valueSchema;
    }

    @Override
    public Envelope getEnvelopeSchema() {
        return envelopeSchema;
    }

    public Struct keyFromPath(String path) {
        return new Struct(keySchema).put(PATH, path);
    }

    /**
     * Builds the metadata struct for a single file. A {@code null} entry (an unknown before-image) yields no struct.
     */
    public Struct metadataFromEntry(FileEntry entry) {
        if (entry == null) {
            return null;
        }
        final Struct struct = new Struct(metadataSchema).put(PATH, entry.path());
        if (entry.size() != FileEntry.UNKNOWN) {
            struct.put(SIZE, entry.size());
        }
        if (entry.modificationTimeMs() != FileEntry.UNKNOWN) {
            struct.put(MTIME, entry.modificationTimeMs());
        }
        if (entry.mode() != null) {
            struct.put(MODE, entry.mode());
        }
        if (entry.owner() != null) {
            struct.put(OWNER, entry.owner());
        }
        if (entry.checksum() != null) {
            struct.put(CHECKSUM, entry.checksum());
        }
        return struct;
    }

    @Override
    public int hashCode() {
        return valueSchema.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof FilesystemCollectionSchema that) {
            return Objects.equals(this.keySchema, that.keySchema) && Objects.equals(this.valueSchema, that.valueSchema);
        }
        return false;
    }

    @Override
    public String toString() {
        return "{ key : " + SchemaUtil.asString(keySchema) + ", value : " + SchemaUtil.asString(valueSchema) + " }";
    }
}
