
package cl.duoc.barriodigital.kafkaadmin.dto;

import java.util.List;
import java.util.Map;

public final class KafkaAdminResponses {

    private KafkaAdminResponses() {
    }

    public record TopicSummary(
            String name,
            int partitions,
            int replicationFactor
    ) {
    }

    public record PartitionDetail(
            int partition,
            int leader,
            List<Integer> replicas,
            List<Integer> inSyncReplicas
    ) {
    }

    public record TopicDetail(
            String name,
            int partitions,
            int replicationFactor,
            List<PartitionDetail> partitionDetails,
            Map<String, String> configurations
    ) {
    }

    public record ConsumerGroupInfo(
            String groupId
    ) {
    }

    public record PartitionLag(
            String topic,
            int partition,
            long committedOffset,
            long logEndOffset,
            long lag
    ) {
    }

    public record GroupLag(
            String groupId,
            long totalLag,
            List<PartitionLag> partitions
    ) {
    }

    public record OperationResponse(
            String message,
            String topic
    ) {
    }
}
