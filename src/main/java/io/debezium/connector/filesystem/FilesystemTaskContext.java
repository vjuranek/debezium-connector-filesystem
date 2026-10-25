/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.connector.common.CdcSourceTaskContext;

/**
 * The context of a {@link FilesystemConnectorTask}, carrying the parsed configuration and the logical server name.
 */
public class FilesystemTaskContext extends CdcSourceTaskContext<FilesystemConnectorConfig> {

    private final String serverName;

    public FilesystemTaskContext(Configuration config) {
        super(config,
                new FilesystemConnectorConfig(config),
                new FilesystemConnectorConfig(config).getCustomMetricTags());
        this.serverName = config.getString(CommonConnectorConfig.TOPIC_PREFIX);
    }

    public String getServerName() {
        return serverName;
    }
}
