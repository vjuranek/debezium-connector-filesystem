/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

/**
 * The kind of change detected for a file between two filesystem snapshots.
 */
public enum ChangeType {

    /** The file exists at snapshot time and is reported during the initial snapshot (maps to Envelope READ). */
    READ,

    /** The file was newly created between two snapshots (maps to Envelope CREATE). */
    CREATED,

    /** The file content or metadata changed between two snapshots (maps to Envelope UPDATE). */
    MODIFIED,

    /** The file was removed between two snapshots (maps to Envelope DELETE). */
    DELETED,

    /** The file was renamed/moved between two snapshots (maps to Envelope UPDATE with a changed key). */
    RENAMED
}
