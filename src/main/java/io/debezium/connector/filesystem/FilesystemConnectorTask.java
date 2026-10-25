/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.filesystem;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.bean.StandardBeanNames;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.config.Configuration;
import io.debezium.config.Field;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.base.QueueProviderService;
import io.debezium.connector.common.BaseSourceTask;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.common.DebeziumHeaderProducer;
import io.debezium.document.DocumentReader;
import io.debezium.pipeline.ChangeEventSourceCoordinator;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.metrics.DefaultChangeEventSourceMetricsFactory;
import io.debezium.pipeline.notification.NotificationService;
import io.debezium.pipeline.signal.SignalProcessor;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.schema.SchemaFactory;
import io.debezium.schema.SchemaNameAdjuster;
import io.debezium.snapshot.SnapshotterService;
import io.debezium.spi.topic.TopicNamingStrategy;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext.PreviousContext;

/**
 * A Kafka Connect source task that observes a copy-on-write filesystem through its snapshot machinery and emits
 * metadata-only change events. A single task watches the configured root; the snapshot phase walks the current tree
 * and the streaming phase diffs successive snapshots.
 * <p>
 * Mirrors {@code MongoDbConnectorTask} (the canonical non-relational template), minus the MongoDB-specific
 * connection, guardrail and shard-offset handling.
 */
public final class FilesystemConnectorTask extends BaseSourceTask<FilesystemPartition, FilesystemOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FilesystemConnectorTask.class);

    private static final String CONTEXT_NAME = "filesystem-connector-task";

    private volatile ChangeEventQueue<DataChangeEvent> queue;
    private volatile FilesystemTaskContext taskContext;
    private volatile ErrorHandler errorHandler;
    private volatile FilesystemSchema schema;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public CdcSourceTaskContext<? extends CommonConnectorConfig> preStart(Configuration config) {
        this.taskContext = new FilesystemTaskContext(config);
        return taskContext;
    }

    @Override
    public ChangeEventSourceCoordinator<FilesystemPartition, FilesystemOffsetContext> start(Configuration config) {
        final FilesystemConnectorConfig connectorConfig = new FilesystemConnectorConfig(config);
        final SchemaNameAdjuster schemaNameAdjuster = connectorConfig.schemaNameAdjuster();

        @SuppressWarnings("unchecked")
        final TopicNamingStrategy<FilesystemCollectionId> topicNamingStrategy = connectorConfig
                .getTopicNamingStrategy(FilesystemConnectorConfig.TOPIC_NAMING_STRATEGY);

        final Schema structSchema = connectorConfig.getSourceInfoStructMaker().schema();
        this.schema = new FilesystemSchema(topicNamingStrategy, structSchema, schemaNameAdjuster);

        final Offsets<FilesystemPartition, FilesystemOffsetContext> previousOffsets = getPreviousOffsets(
                new FilesystemPartition.Provider(connectorConfig),
                new FilesystemOffsetContext.Loader(connectorConfig));
        final Clock clock = Clock.system();

        final PreviousContext previousLogContext = taskContext.configureLoggingContext(CONTEXT_NAME);

        try {
            registerServiceProviders(connectorConfig.getServiceRegistry());

            connectorConfig.getBeanRegistry().add(StandardBeanNames.CONNECTOR_CONFIG, connectorConfig);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.DATABASE_SCHEMA, schema);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.OFFSETS, previousOffsets);
            connectorConfig.getBeanRegistry().add(StandardBeanNames.CDC_SOURCE_TASK_CONTEXT, taskContext);

            this.queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                    .pollInterval(connectorConfig.getPollInterval())
                    .pollDispatchInterval(connectorConfig.getPollDispatchInterval())
                    .maxBatchSize(connectorConfig.getMaxBatchSize())
                    .maxQueueSize(connectorConfig.getMaxQueueSize())
                    .maxQueueSizeInBytes(connectorConfig.getMaxQueueSizeInBytes())
                    .queueProvider(connectorConfig.getServiceRegistry().tryGetService(QueueProviderService.class).getQueueProvider())
                    .loggingContextSupplier(() -> taskContext.configureLoggingContext(CONTEXT_NAME))
                    .build();

            this.errorHandler = new FilesystemErrorHandler(connectorConfig, queue, errorHandler);

            final FilesystemEventMetadataProvider metadataProvider = new FilesystemEventMetadataProvider();

            final SignalProcessor<FilesystemPartition, FilesystemOffsetContext> signalProcessor = new SignalProcessor<>(
                    FilesystemConnector.class, connectorConfig, Map.of(),
                    getAvailableSignalChannels(),
                    DocumentReader.defaultReader(),
                    previousOffsets);

            final SnapshotterService snapshotterService = connectorConfig.getServiceRegistry().tryGetService(SnapshotterService.class);

            final EventDispatcher<FilesystemPartition, FilesystemCollectionId> dispatcher = new EventDispatcher<>(
                    connectorConfig,
                    topicNamingStrategy,
                    schema,
                    queue,
                    x -> true,
                    DataChangeEvent::new,
                    metadataProvider,
                    schemaNameAdjuster,
                    signalProcessor,
                    connectorConfig.getServiceRegistry().tryGetService(DebeziumHeaderProducer.class));

            final NotificationService<FilesystemPartition, FilesystemOffsetContext> notificationService = new NotificationService<>(
                    getNotificationChannels(), connectorConfig, SchemaFactory.get(), dispatcher::enqueueNotification);

            final DefaultChangeEventSourceMetricsFactory<FilesystemPartition> metricsFactory = new DefaultChangeEventSourceMetricsFactory<>();

            final ChangeEventSourceCoordinator<FilesystemPartition, FilesystemOffsetContext> coordinator = new ChangeEventSourceCoordinator<>(
                    previousOffsets,
                    errorHandler,
                    FilesystemConnector.class,
                    connectorConfig,
                    new FilesystemChangeEventSourceFactory(connectorConfig, dispatcher, clock, snapshotterService),
                    metricsFactory,
                    dispatcher,
                    schema,
                    signalProcessor,
                    notificationService,
                    snapshotterService);

            coordinator.start(taskContext, this.queue, metadataProvider);

            return coordinator;
        }
        finally {
            previousLogContext.restore();
        }
    }

    @Override
    protected String connectorName() {
        return Module.name();
    }

    @Override
    public List<SourceRecord> doPoll() throws InterruptedException {
        return pollRecords(queue);
    }

    @Override
    protected Optional<ErrorHandler> getErrorHandler() {
        return Optional.ofNullable(errorHandler);
    }

    @Override
    public void doStop() {
        final PreviousContext previousLogContext = taskContext.configureLoggingContext(CONTEXT_NAME);
        try {
            if (schema != null) {
                schema.close();
            }
            if (queue != null) {
                queue.close();
            }
        }
        finally {
            previousLogContext.restore();
        }
    }

    @Override
    protected Iterable<Field> getAllConfigurationFields() {
        return FilesystemConnectorConfig.ALL_FIELDS;
    }
}
