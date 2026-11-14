
package cl.duoc.barriodigital.kafkaadmin.controller;

import java.util.List;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import cl.duoc.barriodigital.kafkaadmin.dto.KafkaAdminRequests;
import cl.duoc.barriodigital.kafkaadmin.dto.KafkaAdminResponses;
import cl.duoc.barriodigital.kafkaadmin.service.KafkaAdminService;
import jakarta.validation.Valid;

@RestController
@RequestMapping("/api/v1/kafka-admin")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    @GetMapping("/topics")
    public List<KafkaAdminResponses.TopicSummary> listTopics() {
        return service.listTopics();
    }

    @GetMapping("/topics/{name}")
    public KafkaAdminResponses.TopicDetail getTopic(
            @PathVariable String name) {

        return service.getTopic(name);
    }

    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public KafkaAdminResponses.OperationResponse createTopic(
            @Valid @RequestBody
            KafkaAdminRequests.CreateTopic request) {

        return service.createTopic(request);
    }

    @PutMapping("/topics/{name}/partitions")
    public KafkaAdminResponses.OperationResponse increasePartitions(
            @PathVariable String name,
            @Valid @RequestBody
            KafkaAdminRequests.ChangePartitions request) {

        return service.increasePartitions(name, request);
    }

    @PutMapping("/topics/{name}/retention")
    public KafkaAdminResponses.OperationResponse updateRetention(
            @PathVariable String name,
            @Valid @RequestBody
            KafkaAdminRequests.ChangeRetention request) {

        return service.updateRetention(name, request);
    }

    @DeleteMapping("/topics/{name}")
    public KafkaAdminResponses.OperationResponse deleteTopic(
            @PathVariable String name) {

        return service.deleteTopic(name);
    }

    @GetMapping("/consumer-groups")
    public List<KafkaAdminResponses.ConsumerGroupInfo>
            listConsumerGroups() {

        return service.listConsumerGroups();
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public KafkaAdminResponses.GroupLag getGroupLag(
            @PathVariable String groupId) {

        return service.getGroupLag(groupId);
    }
}
