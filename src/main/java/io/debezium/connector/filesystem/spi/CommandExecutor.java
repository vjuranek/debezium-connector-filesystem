/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem.spi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import io.debezium.DebeziumException;

/**
 * Runs an external command and returns its standard output. Abstracted so that {@link FilesystemSnapshotProvider}
 * implementations can be unit-tested with recorded {@code zfs}/{@code btrfs} output instead of a real filesystem.
 */
@FunctionalInterface
public interface CommandExecutor {

    /**
     * Executes the given command and returns its standard output as a single string.
     *
     * @param command the command and its arguments
     * @return the captured standard output
     */
    String execute(List<String> command);

    /**
     * Default implementation backed by {@link ProcessBuilder}. Fails with a {@link DebeziumException} when the
     * command exits with a non-zero status, surfacing the captured output for diagnostics.
     */
    static CommandExecutor processBuilder() {
        return command -> {
            try {
                Process process = new ProcessBuilder(command)
                        .redirectErrorStream(false)
                        .start();

                String stdout;
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                    stdout = reader.lines().collect(Collectors.joining("\n"));
                }

                String stderr;
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
                    stderr = reader.lines().collect(Collectors.joining("\n"));
                }

                int exitCode = process.waitFor();
                if (exitCode != 0) {
                    throw new DebeziumException("Command " + command + " failed with exit code " + exitCode + ": " + stderr);
                }
                return stdout;
            }
            catch (IOException e) {
                throw new DebeziumException("Failed to execute command " + command, e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new DebeziumException("Interrupted while executing command " + command, e);
            }
        };
    }
}
