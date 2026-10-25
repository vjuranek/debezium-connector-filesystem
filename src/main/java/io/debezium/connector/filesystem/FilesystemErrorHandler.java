/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Collect;

/**
 * Error handler for the filesystem connector. I/O failures when shelling out to the filesystem tooling are treated
 * as retriable so a transient problem (a busy mount, a momentarily unavailable snapshot) restarts the task rather
 * than failing it.
 */
public class FilesystemErrorHandler extends ErrorHandler {

    public FilesystemErrorHandler(FilesystemConnectorConfig connectorConfig, ChangeEventQueue<?> queue, ErrorHandler replacedErrorHandler) {
        super(FilesystemConnector.class, connectorConfig, queue, replacedErrorHandler);
    }

    @Override
    protected Set<Class<? extends Exception>> communicationExceptions() {
        return Collect.unmodifiableSet(IOException.class, UncheckedIOException.class);
    }
}
