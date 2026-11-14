
package cl.duoc.barriodigital.kafkaadmin.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;

public final class KafkaAdminRequests {

    private KafkaAdminRequests() {
    }

    public record CreateTopic(
            @NotBlank
            @Pattern(regexp = "bd\\.demo\\.[A-Za-z0-9._-]{1,100}")
            String name,

            @Min(1)
            @Max(12)
            int partitions,

            @Min(1)
            @Max(3)
            short replicationFactor
    ) {
    }

    public record ChangePartitions(
            @Min(2)
            @Max(24)
            int totalPartitions
    ) {
    }

    public record ChangeRetention(
            @NotNull
            @Min(60000)
            Long retentionMs
    ) {
    }
}
