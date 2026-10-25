/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.time.Instant;

import io.debezium.annotation.NotThreadSafe;
import io.debezium.connector.SnapshotRecord;
import io.debezium.connector.common.BaseSourceInfo;

/**
 * Coordinates that identify the position of a filesystem change event. The {@code cursor} plays the role of an
 * LSN: it is the snapshot id / ZFS birth TXG / Btrfs generation the event was observed at, and it is persisted
 * in the offset so streaming resumes from it after a restart.
 */
@NotThreadSafe
public class SourceInfo extends BaseSourceInfo {

    public static final String FILESYSTEM_TYPE_KEY = "fs_type";
    public static final String PATH_KEY = "path";
    public static final String CURSOR_KEY = "cursor";

    private final String filesystemType;
    private final String rootPath;

    private String cursor;
    private String path;
    private Instant timestamp;

    protected SourceInfo(FilesystemConnectorConfig connectorConfig) {
        super(connectorConfig);
        this.filesystemType = connectorConfig.getFilesystemType();
        this.rootPath = connectorConfig.getFilesystemPath();
    }

    public String filesystemType() {
        return filesystemType;
    }

    public String rootPath() {
        return rootPath;
    }

    public String cursor() {
        return cursor;
    }

    public void setCursor(String cursor) {
        this.cursor = cursor;
    }

    public String path() {
        return path;
    }

    /**
     * Records the file and time a change was observed for.
     *
     * @param path the path of the changed file
     * @param timestamp the time the change was observed
     */
    public void event(String path, Instant timestamp) {
        this.path = path;
        this.timestamp = timestamp;
    }

    @Override
    protected Instant timestamp() {
        return timestamp;
    }

    @Override
    protected String database() {
        return rootPath;
    }

    public void startInitialSnapshot() {
        setSnapshot(SnapshotRecord.TRUE);
    }

    @Override
    public String toString() {
        return "SourceInfo [filesystemType=" + filesystemType + ", rootPath=" + rootPath + ", cursor=" + cursor
                + ", path=" + path + ", snapshot=" + snapshotRecord + "]";
    }
}
