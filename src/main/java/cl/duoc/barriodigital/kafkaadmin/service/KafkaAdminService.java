
package cl.duoc.barriodigital.kafkaadmin.service;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

import cl.duoc.barriodigital.kafkaadmin.dto.KafkaAdminRequests;
import cl.duoc.barriodigital.kafkaadmin.dto.KafkaAdminResponses;

@Service
public class KafkaAdminService {

    private final AdminClient adminClient;
    private final boolean writeEnabled;
    private final int timeoutSeconds;

    public KafkaAdminService(
            AdminClient adminClient,
            @Value("${barriodigital.kafka-admin.write-enabled:false}")
            boolean writeEnabled,
            @Value("${barriodigital.kafka-admin.timeout-seconds:10}")
            int timeoutSeconds) {

        this.adminClient = adminClient;
        this.writeEnabled = writeEnabled;
        this.timeoutSeconds = timeoutSeconds;
    }

    // ===================================================
    // LISTAR TOPICOS
    // ===================================================

    public List<KafkaAdminResponses.TopicSummary> listTopics() {

        Set<String> names =
                await(adminClient.listTopics().names());

        if (names.isEmpty()) {
            return List.of();
        }

        Map<String, TopicDescription> descriptions =
                await(adminClient.describeTopics(names).allTopicNames());

        return descriptions.values()
                .stream()
                .sorted(Comparator.comparing(TopicDescription::name))
                .map(topic -> new KafkaAdminResponses.TopicSummary(
                        topic.name(),
                        topic.partitions().size(),
                        replicationFactor(topic)
                ))
                .toList();
    }

    // ===================================================
    // CONSULTAR TOPICO
    // ===================================================

    public KafkaAdminResponses.TopicDetail getTopic(String name) {

        TopicDescription topic =
                getTopicDescription(name);

        List<KafkaAdminResponses.PartitionDetail> partitions =
                topic.partitions()
                        .stream()
                        .map(this::partitionDetail)
                        .toList();

        ConfigResource resource =
                new ConfigResource(
                        ConfigResource.Type.TOPIC,
                        name
                );

        var configMap = await(
                adminClient
                        .describeConfigs(List.of(resource))
                        .all()
        );

        Map<String, String> settings = new LinkedHashMap<>();

        var config = configMap.get(resource);

        if (config != null) {
            for (String key : List.of(
                    "retention.ms",
                    "cleanup.policy",
                    "min.insync.replicas")) {

                ConfigEntry entry = config.get(key);

                if (entry != null && entry.value() != null) {
                    settings.put(key, entry.value());
                }
            }
        }

        return new KafkaAdminResponses.TopicDetail(
                topic.name(),
                topic.partitions().size(),
                replicationFactor(topic),
                partitions,
                settings
        );
    }

    // ===================================================
    // CREAR TOPICO DE DEMOSTRACION
    // ===================================================

    public KafkaAdminResponses.OperationResponse createTopic(
            KafkaAdminRequests.CreateTopic request) {

        requireWritePermission(request.name());

        NewTopic topic = new NewTopic(
                request.name(),
                request.partitions(),
                request.replicationFactor()
        );

        await(
                adminClient
                        .createTopics(List.of(topic))
                        .all()
        );

        return new KafkaAdminResponses.OperationResponse(
                "Topico creado correctamente",
                request.name()
        );
    }

    // ===================================================
    // AUMENTAR PARTICIONES
    // ===================================================

    public KafkaAdminResponses.OperationResponse increasePartitions(
            String name,
            KafkaAdminRequests.ChangePartitions request) {

        requireWritePermission(name);

        TopicDescription topic = getTopicDescription(name);

        if (request.totalPartitions() <= topic.partitions().size()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "El nuevo total debe superar las particiones actuales"
            );
        }

        await(
                adminClient
                        .createPartitions(
                                Map.of(
                                        name,
                                        NewPartitions.increaseTo(
                                                request.totalPartitions()
                                        )
                                )
                        )
                        .all()
        );

        return new KafkaAdminResponses.OperationResponse(
                "Particiones actualizadas",
                name
        );
    }

    // ===================================================
    // MODIFICAR RETENCION
    // ===================================================

    public KafkaAdminResponses.OperationResponse updateRetention(
            String name,
            KafkaAdminRequests.ChangeRetention request) {

        requireWritePermission(name);

        ConfigResource resource = new ConfigResource(
                ConfigResource.Type.TOPIC,
                name
        );

        AlterConfigOp operation = new AlterConfigOp(
                new ConfigEntry(
                        "retention.ms",
                        request.retentionMs().toString()
                ),
                AlterConfigOp.OpType.SET
        );

        Map<ConfigResource, Collection<AlterConfigOp>> changes =
                Map.of(resource, List.of(operation));

        await(
                adminClient
                        .incrementalAlterConfigs(changes)
                        .all()
        );

        return new KafkaAdminResponses.OperationResponse(
                "Retencion actualizada",
                name
        );
    }

    // ===================================================
    // ELIMINAR TOPICO DE DEMOSTRACION
    // ===================================================

    public KafkaAdminResponses.OperationResponse deleteTopic(
            String name) {

        requireWritePermission(name);

        await(
                adminClient
                        .deleteTopics(List.of(name))
                        .all()
        );

        return new KafkaAdminResponses.OperationResponse(
                "Topico eliminado",
                name
        );
    }

    // ===================================================
    // LISTAR CONSUMER GROUPS
    // ===================================================

    @SuppressWarnings("deprecation")
    public List<KafkaAdminResponses.ConsumerGroupInfo>
            listConsumerGroups() {

        var groups = await(
                adminClient.listConsumerGroups().all()
        );

        return groups.stream()
                .map(group ->
                        new KafkaAdminResponses.ConsumerGroupInfo(
                                group.groupId()
                        )
                )
                .sorted(Comparator.comparing(
                        KafkaAdminResponses.ConsumerGroupInfo::groupId
                ))
                .toList();
    }

    // ===================================================
    // CONSULTAR OFFSETS Y LAG DE UN GRUPO
    // ===================================================

    public KafkaAdminResponses.GroupLag getGroupLag(
            String groupId) {

        if (groupId == null || groupId.isBlank()) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "Consumer group requerido"
            );
        }

        Map<TopicPartition, OffsetAndMetadata> committed =
                await(
                        adminClient
                                .listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata()
                );

        if (committed.isEmpty()) {
            return new KafkaAdminResponses.GroupLag(
                    groupId,
                    0L,
                    List.of()
            );
        }

        Map<TopicPartition, OffsetSpec> requests = new HashMap<>();

        for (TopicPartition partition : committed.keySet()) {
            requests.put(
                    partition,
                    OffsetSpec.latest()
            );
        }

        var endOffsets = await(
                adminClient.listOffsets(requests).all()
        );

        List<KafkaAdminResponses.PartitionLag> partitions =
                committed.entrySet()
                        .stream()
                        .sorted(
                                Comparator
                                        .comparing(
                                                (Map.Entry<TopicPartition, OffsetAndMetadata> e)
                                                        -> e.getKey().topic()
                                        )
                                        .thenComparingInt(
                                                e -> e.getKey().partition()
                                        )
                        )
                        .map(entry -> {

                            TopicPartition partition = entry.getKey();

                            long current =
                                    entry.getValue().offset();

                            long end =
                                    endOffsets.get(partition).offset();

                            long lag = Math.max(
                                    0L,
                                    end - current
                            );

                            return new KafkaAdminResponses.PartitionLag(
                                    partition.topic(),
                                    partition.partition(),
                                    current,
                                    end,
                                    lag
                            );
                        })
                        .toList();

        long totalLag = partitions.stream()
                .mapToLong(
                        KafkaAdminResponses.PartitionLag::lag
                )
                .sum();

        return new KafkaAdminResponses.GroupLag(
                groupId,
                totalLag,
                partitions
        );
    }

    // ===================================================
    // METODOS AUXILIARES
    // ===================================================

    private TopicDescription getTopicDescription(String name) {

        return await(
                adminClient
                        .describeTopics(List.of(name))
                        .topicNameValues()
                        .get(name)
        );
    }

    private int replicationFactor(TopicDescription topic) {

        if (topic.partitions().isEmpty()) {
            return 0;
        }

        return topic.partitions()
                .get(0)
                .replicas()
                .size();
    }

    private KafkaAdminResponses.PartitionDetail partitionDetail(
            TopicPartitionInfo partition) {

        return new KafkaAdminResponses.PartitionDetail(
                partition.partition(),
                partition.leader() == null
                        ? -1
                        : partition.leader().id(),
                partition.replicas()
                        .stream()
                        .map(Node::id)
                        .toList(),
                partition.isr()
                        .stream()
                        .map(Node::id)
                        .toList()
        );
    }

    private void requireWritePermission(String topic) {

        if (!writeEnabled) {
            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Administracion de escritura deshabilitada"
            );
        }

        if (topic == null ||
                !topic.matches("bd\\.demo\\.[A-Za-z0-9._-]{1,100}")) {

            throw new ResponseStatusException(
                    HttpStatus.FORBIDDEN,
                    "Solo se permiten topicos bd.demo.*"
            );
        }
    }

    private <T> T await(KafkaFuture<T> future) {

        try {
            return future.get(
                    timeoutSeconds,
                    TimeUnit.SECONDS
            );

        } catch (InterruptedException ex) {

            Thread.currentThread().interrupt();

            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "Operacion Kafka interrumpida",
                    ex
            );

        } catch (TimeoutException ex) {

            throw new ResponseStatusException(
                    HttpStatus.GATEWAY_TIMEOUT,
                    "Kafka no respondio dentro del tiempo configurado",
                    ex
            );

        } catch (ExecutionException ex) {

            Throwable cause = ex.getCause();

            if (cause instanceof UnknownTopicOrPartitionException) {
                throw new ResponseStatusException(
                        HttpStatus.NOT_FOUND,
                        "Topico no encontrado",
                        cause
                );
            }

            if (cause instanceof TopicExistsException) {
                throw new ResponseStatusException(
                        HttpStatus.CONFLICT,
                        "El topico ya existe",
                        cause
                );
            }

            if (cause instanceof InvalidPartitionsException ||
                    cause instanceof InvalidReplicationFactorException) {

                throw new ResponseStatusException(
                        HttpStatus.BAD_REQUEST,
                        "Configuracion Kafka invalida",
                        cause
                );
            }

            throw new ResponseStatusException(
                    HttpStatus.SERVICE_UNAVAILABLE,
                    "No fue posible completar la operacion Kafka",
                    cause
            );
        }
    }
}
