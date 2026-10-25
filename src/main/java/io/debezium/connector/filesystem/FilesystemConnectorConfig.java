/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import io.debezium.DebeziumException;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.ConfigDefinition;
import io.debezium.config.Configuration;
import io.debezium.config.EnumeratedValue;
import io.debezium.config.Field;
import io.debezium.connector.AbstractSourceInfo;
import io.debezium.connector.SourceInfoStructMaker;
import io.debezium.connector.filesystem.btrfs.BtrfsSnapshotProvider;
import io.debezium.connector.filesystem.spi.CommandExecutor;
import io.debezium.connector.filesystem.spi.FilesystemSnapshotProvider;
import io.debezium.connector.filesystem.zfs.ZfsSnapshotProvider;

/**
 * The configuration of a {@link FilesystemConnector}.
 * <p>
 * Mirrors the non-relational configuration shape of the MongoDB connector: it extends
 * {@link CommonConnectorConfig} directly (not the relational base) and adds the filesystem-specific options that
 * select and point at a pluggable {@link FilesystemSnapshotProvider} backend.
 */
public class FilesystemConnectorConfig extends CommonConnectorConfig {

    public static final String TYPE_BTRFS = "btrfs";
    public static final String TYPE_ZFS = "zfs";

    private static final String DEFAULT_COLLECTION_NAME = "files";
    private static final int DEFAULT_SNAPSHOT_POLL_INTERVAL_MS = 60_000;

    /**
     * The set of predefined snapshot modes. The {@link #getValue() value} of each is intentionally the name of a
     * core {@code Snapshotter} so the shared {@code SnapshotterService} resolves it without a connector-specific
     * snapshotter implementation.
     */
    public enum SnapshotMode implements EnumeratedValue {

        /** Walk the current tree once on first start, then stream diffs. */
        INITIAL("initial"),
        /** Walk the current tree on every start, then stream diffs. */
        ALWAYS("always"),
        /** Walk the current tree once and then stop. */
        INITIAL_ONLY("initial_only"),
        /** Skip the initial walk; only stream diffs from the current cursor. */
        NO_DATA("no_data"),
        /** Walk the tree only when the stored cursor is no longer usable. */
        WHEN_NEEDED("when_needed"),
        /** Inject a custom snapshotter. */
        CUSTOM("custom");

        private final String value;

        SnapshotMode(String value) {
            this.value = value;
        }

        @Override
        public String getValue() {
            return value;
        }

        public static SnapshotMode parse(String value) {
            if (value == null) {
                return null;
            }
            value = value.trim();
            for (SnapshotMode option : SnapshotMode.values()) {
                if (option.getValue().equalsIgnoreCase(value)) {
                    return option;
                }
            }
            return null;
        }
    }

    public static final Field FILESYSTEM_TYPE = Field.create("filesystem.type")
            .withDisplayName("Filesystem type")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("The copy-on-write filesystem backend to use: '" + TYPE_BTRFS + "' or '" + TYPE_ZFS + "'.");

    public static final Field FILESYSTEM_PATH = Field.create("filesystem.path")
            .withDisplayName("Watched root path")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.HIGH)
            .required()
            .withDescription("For btrfs, the absolute path of the subvolume to watch. For zfs, the mount root of the dataset "
                    + "(its '.zfs/snapshot' directory is used to enumerate snapshots).");

    public static final Field FILESYSTEM_DATASET = Field.create("filesystem.dataset")
            .withDisplayName("ZFS dataset")
            .withType(Type.STRING)
            .withWidth(Width.LONG)
            .withImportance(Importance.MEDIUM)
            .withDescription("The ZFS dataset name (for example 'pool/data'); required when 'filesystem.type' is 'zfs'.");

    public static final Field COLLECTION_NAME = Field.create("filesystem.collection")
            .withDisplayName("Logical collection name")
            .withType(Type.STRING)
            .withWidth(Width.MEDIUM)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_COLLECTION_NAME)
            .withDescription("The logical collection name the watched files are grouped under; drives the topic name.");

    public static final Field SNAPSHOT_POLL_INTERVAL_MS = Field.create("snapshot.poll.interval.ms")
            .withDisplayName("Snapshot poll interval (ms)")
            .withType(Type.LONG)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(DEFAULT_SNAPSHOT_POLL_INTERVAL_MS)
            .withValidation(Field::isPositiveInteger)
            .withDescription("How often, in milliseconds, the streaming phase polls for a newer snapshot/generation and diffs it.");

    public static final Field CHECKSUM_ENABLED = Field.create("checksum.enabled")
            .withDisplayName("Compute content checksum")
            .withType(Type.BOOLEAN)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(false)
            .withDescription("Whether to compute a SHA-256 checksum of each file's content and include it in the event. "
                    + "Disabled by default because it reads file content and is expensive for large trees.");

    public static final Field SNAPSHOT_MODE = Field.create("snapshot.mode")
            .withDisplayName("Snapshot mode")
            .withType(Type.STRING)
            .withWidth(Width.SHORT)
            .withImportance(Importance.LOW)
            .withDefault(SnapshotMode.INITIAL.getValue())
            .withDescription("The criteria for running the initial tree walk. Options are 'initial' (default), 'always', "
                    + "'initial_only', 'no_data', 'when_needed' and 'custom'.");

    public static final Field SOURCE_INFO_STRUCT_MAKER = CommonConnectorConfig.SOURCE_INFO_STRUCT_MAKER
            .withDefault(FilesystemSourceInfoStructMaker.class.getName());

    private static final ConfigDefinition CONFIG_DEFINITION = CommonConnectorConfig.CONFIG_DEFINITION.edit()
            .name("Filesystem")
            .group(Field.Group.CONNECTION, FILESYSTEM_TYPE, FILESYSTEM_PATH, FILESYSTEM_DATASET)
            .group(Field.Group.CONNECTOR, TOPIC_PREFIX, SNAPSHOT_MODE, SNAPSHOT_POLL_INTERVAL_MS, CHECKSUM_ENABLED, COLLECTION_NAME, SOURCE_INFO_STRUCT_MAKER)
            .create();

    /**
     * The set of {@link Field}s defined as part of this configuration.
     */
    public static final Field.Set ALL_FIELDS = Field.setOf(CONFIG_DEFINITION.all());

    public static ConfigDef configDef() {
        return CONFIG_DEFINITION.configDef();
    }

    private final String filesystemType;
    private final String filesystemPath;
    private final String dataset;
    private final String collectionName;
    private final Duration snapshotPollInterval;
    private final boolean checksumEnabled;
    private final SnapshotMode snapshotMode;

    public FilesystemConnectorConfig(Configuration config) {
        super(config, 0);
        this.filesystemType = config.getString(FILESYSTEM_TYPE);
        this.filesystemPath = config.getString(FILESYSTEM_PATH);
        this.dataset = config.getString(FILESYSTEM_DATASET);
        this.collectionName = config.getString(COLLECTION_NAME);
        this.snapshotPollInterval = Duration.ofMillis(config.getLong(SNAPSHOT_POLL_INTERVAL_MS));
        this.checksumEnabled = config.getBoolean(CHECKSUM_ENABLED);
        this.snapshotMode = SnapshotMode.parse(config.getString(SNAPSHOT_MODE));
    }

    public String getFilesystemType() {
        return filesystemType;
    }

    public String getFilesystemPath() {
        return filesystemPath;
    }

    public String getDataset() {
        return dataset;
    }

    public String getCollectionName() {
        return collectionName;
    }

    public FilesystemCollectionId getCollectionId() {
        return new FilesystemCollectionId(collectionName);
    }

    public Duration getSnapshotPollInterval() {
        return snapshotPollInterval;
    }

    public boolean isChecksumEnabled() {
        return checksumEnabled;
    }

    @Override
    public SnapshotMode getSnapshotMode() {
        return snapshotMode;
    }

    @Override
    public Optional<EnumeratedValue> getSnapshotLockingMode() {
        return Optional.empty();
    }

    @Override
    public String getContextName() {
        return Module.contextName();
    }

    @Override
    public String getConnectorName() {
        return Module.name();
    }

    @Override
    protected SourceInfoStructMaker<? extends AbstractSourceInfo> getSourceInfoStructMaker(Version version) {
        return getSourceInfoStructMaker(SOURCE_INFO_STRUCT_MAKER, Module.name(), Module.version(), this);
    }

    /**
     * Builds the pluggable backend selected by {@link #FILESYSTEM_TYPE}. The caller owns the returned instance and
     * must {@link FilesystemSnapshotProvider#close() close} it.
     */
    public FilesystemSnapshotProvider createSnapshotProvider() {
        final CommandExecutor executor = CommandExecutor.processBuilder();
        if (TYPE_BTRFS.equalsIgnoreCase(filesystemType)) {
            return new BtrfsSnapshotProvider(Path.of(filesystemPath), checksumEnabled, executor);
        }
        if (TYPE_ZFS.equalsIgnoreCase(filesystemType)) {
            if (dataset == null || dataset.isBlank()) {
                throw new DebeziumException("'" + FILESYSTEM_DATASET.name() + "' is required when '" + FILESYSTEM_TYPE.name() + "' is '" + TYPE_ZFS + "'");
            }
            return new ZfsSnapshotProvider(dataset, Path.of(filesystemPath), checksumEnabled, executor);
        }
        throw new DebeziumException("Unsupported '" + FILESYSTEM_TYPE.name() + "' value: " + filesystemType
                + " (expected '" + TYPE_BTRFS + "' or '" + TYPE_ZFS + "')");
    }
}
