/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.connect.connector.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.common.BaseSourceConnector;

/**
 * A Kafka Connect source connector that observes a copy-on-write filesystem (Btrfs or ZFS) through its snapshot
 * machinery and emits metadata-only change events for the files that change between snapshots.
 */
public class FilesystemConnector extends BaseSourceConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemConnector.class);

    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return FilesystemConnectorTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        final Configuration config = Configuration.from(props);
        if (!config.validateAndRecord(FilesystemConnectorConfig.ALL_FIELDS, LOGGER::error)) {
            throw new DebeziumException("Error configuring an instance of " + getClass().getSimpleName() + "; check the logs for details");
        }
        this.properties = props;
        LOGGER.info("Successfully started filesystem connector");
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (properties == null) {
            LOGGER.error("Configuring a maximum of {} tasks with no connector configuration available", maxTasks);
            return Collections.emptyList();
        }
        // A single task watches the configured root.
        return List.of(Map.copyOf(properties));
    }

    @Override
    public void stop() {
        this.properties = null;
    }

    @Override
    public ConfigDef config() {
        return FilesystemConnectorConfig.configDef();
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        final Configuration config = Configuration.from(connectorConfigs);
        return new Config(new java.util.ArrayList<>(validateAllFields(config).values()));
    }

    @Override
    protected Map<String, ConfigValue> validateAllFields(Configuration config) {
        return config.validate(FilesystemConnectorConfig.ALL_FIELDS);
    }
}
