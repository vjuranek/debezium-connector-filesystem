/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

import io.debezium.connector.filesystem.spi.ChangeType;
import io.debezium.data.Envelope.Operation;

/**
 * Unit tests for the mapping from a filesystem {@link ChangeType} to a Debezium {@link Operation}.
 */
public class FilesystemChangeRecordEmitterTest {

    @Test
    public void shouldMapReadToRead() {
        assertThat(FilesystemChangeRecordEmitter.toOperation(ChangeType.READ)).isEqualTo(Operation.READ);
    }

    @Test
    public void shouldMapCreatedToCreate() {
        assertThat(FilesystemChangeRecordEmitter.toOperation(ChangeType.CREATED)).isEqualTo(Operation.CREATE);
    }

    @Test
    public void shouldMapModifiedToUpdate() {
        assertThat(FilesystemChangeRecordEmitter.toOperation(ChangeType.MODIFIED)).isEqualTo(Operation.UPDATE);
    }

    @Test
    public void shouldMapDeletedToDelete() {
        assertThat(FilesystemChangeRecordEmitter.toOperation(ChangeType.DELETED)).isEqualTo(Operation.DELETE);
    }

    @Test
    public void shouldSurfaceRenameAsCreate() {
        // The PoC surfaces a rename as a create of the new path (no paired delete of the old path).
        assertThat(FilesystemChangeRecordEmitter.toOperation(ChangeType.RENAMED)).isEqualTo(Operation.CREATE);
    }
}
