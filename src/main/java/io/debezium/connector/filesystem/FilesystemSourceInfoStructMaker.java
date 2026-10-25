/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.AbstractSourceInfoStructMaker;

/**
 * Builds the {@code source} block for filesystem change events, adding the filesystem type, watched root path
 * and cursor on top of the fields shared by all Debezium connectors.
 */
public class FilesystemSourceInfoStructMaker extends AbstractSourceInfoStructMaker<SourceInfo> {

    private Schema schema;

    @Override
    public void init(String connector, String version, CommonConnectorConfig connectorConfig) {
        super.init(connector, version, connectorConfig);
        schema = commonSchemaBuilder()
                .name(connectorConfig.schemaNameAdjuster().adjust("io.debezium.connector.filesystem.Source"))
                .field(SourceInfo.FILESYSTEM_TYPE_KEY, Schema.STRING_SCHEMA)
                .field(SourceInfo.PATH_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .field(SourceInfo.CURSOR_KEY, Schema.OPTIONAL_STRING_SCHEMA)
                .build();
    }

    @Override
    public Schema schema() {
        return schema;
    }

    @Override
    public Struct struct(SourceInfo sourceInfo) {
        Struct struct = super.commonStruct(sourceInfo)
                .put(SourceInfo.FILESYSTEM_TYPE_KEY, sourceInfo.filesystemType());

        if (sourceInfo.path() != null) {
            struct.put(SourceInfo.PATH_KEY, sourceInfo.path());
        }
        if (sourceInfo.cursor() != null) {
            struct.put(SourceInfo.CURSOR_KEY, sourceInfo.cursor());
        }
        return struct;
    }
}
