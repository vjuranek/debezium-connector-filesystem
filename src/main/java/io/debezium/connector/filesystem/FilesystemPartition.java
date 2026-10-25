/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import io.debezium.pipeline.spi.Partition;
import io.debezium.util.Collect;

/**
 * Represents the single partition used by the filesystem connector. The connector watches one root per task,
 * so there is exactly one partition keyed by the logical server name (the configured topic prefix).
 */
public class FilesystemPartition implements Partition {

    private static final String SERVER_PARTITION_KEY = "server";

    private final String serverName;

    public FilesystemPartition(String serverName) {
        this.serverName = serverName;
    }

    @Override
    public Map<String, String> getSourcePartition() {
        return Collect.hashMapOf(SERVER_PARTITION_KEY, serverName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(serverName, ((FilesystemPartition) obj).serverName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serverName);
    }

    @Override
    public String toString() {
        return "FilesystemPartition [sourcePartition=" + getSourcePartition() + "]";
    }

    public static class Provider implements Partition.Provider<FilesystemPartition> {

        private final String serverName;

        public Provider(FilesystemConnectorConfig connectorConfig) {
            this.serverName = connectorConfig.getLogicalName();
        }

        @Override
        public Set<FilesystemPartition> getPartitions() {
            return Set.of(new FilesystemPartition(serverName));
        }
    }
}
