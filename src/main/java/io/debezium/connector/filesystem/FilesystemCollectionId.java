/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.List;
import java.util.Objects;

import io.debezium.spi.schema.DataCollectionId;

/**
 * Identifies the logical grouping of files produced by a single watched root (a Btrfs subvolume or a ZFS
 * dataset). It is the filesystem connector's analog of a relational table or a MongoDB collection and drives
 * topic naming through the configured {@code TopicNamingStrategy}.
 */
public class FilesystemCollectionId implements DataCollectionId {

    private final String name;

    public FilesystemCollectionId(String name) {
        this.name = Objects.requireNonNull(name, "Collection name must not be null");
    }

    public String name() {
        return name;
    }

    @Override
    public String identifier() {
        return name;
    }

    @Override
    public List<String> parts() {
        return List.of(name);
    }

    @Override
    public List<String> databaseParts() {
        return List.of(name);
    }

    @Override
    public List<String> schemaParts() {
        return List.of(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return Objects.equals(name, ((FilesystemCollectionId) obj).name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
