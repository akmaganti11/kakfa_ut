import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.*;
import java.util.concurrent.ExecutionException;

public class KafkaOffsetTimestamps {

    public static void main(String[] args) {
        // Kafka configuration
        String bootstrapServers = "localhost:9092"; // Replace with your Kafka broker address
        String topicName = "your-topic-name";       // Replace with your Kafka topic name
        String consumerGroup = "your-consumer-group"; // Replace with your consumer group ID

        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(props)) {
            // Fetch topic partitions
            List<TopicPartition> topicPartitions = getTopicPartitions(adminClient, topicName);

            // Fetch latest produced offsets and timestamps
            Map<TopicPartition, Long> producedTimestamps = getOffsetsAndTimestamps(adminClient, topicPartitions, OffsetSpec.latest());

            // Fetch committed offsets for the consumer group
            Map<TopicPartition, Long> committedOffsets = getConsumerGroupOffsets(adminClient, consumerGroup);

            // Fetch timestamps for the committed offsets
            Map<TopicPartition, Long> consumedTimestamps = getOffsetsAndTimestampsForOffset(adminClient, topicPartitions, committedOffsets);

            // Calculate and display time differences
            for (TopicPartition partition : topicPartitions) {
                long producedTimestamp = producedTimestamps.getOrDefault(partition, 0L);
                long consumedTimestamp = consumedTimestamps.getOrDefault(partition, 0L);

                long timeDifference = consumedTimestamp - producedTimestamp;

                System.out.printf(
                        "Partition: %d | Produced Timestamp: %d | Consumed Timestamp: %d | Time Difference (ms): %d%n",
                        partition.partition(), producedTimestamp, consumedTimestamp, timeDifference);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<TopicPartition> getTopicPartitions(AdminClient adminClient, String topicName) throws ExecutionException, InterruptedException {
        // Fetch partitions for the topic
        return adminClient.describeTopics(Collections.singletonList(topicName))
                .allTopicNames()
                .get()
                .get(topicName)
                .partitions()
                .stream()
                .map(partitionInfo -> new TopicPartition(topicName, partitionInfo.partition()))
                .toList();
    }

    private static Map<TopicPartition, Long> getOffsetsAndTimestamps(AdminClient adminClient, List<TopicPartition> topicPartitions, OffsetSpec offsetSpec) throws ExecutionException, InterruptedException {
        // Fetch offsets and their timestamps for the given OffsetSpec
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition partition : topicPartitions) {
            request.put(partition, offsetSpec);
        }

        Map<TopicPartition, Long> timestamps = new HashMap<>();
        adminClient.listOffsets(request).all().get()
                .forEach((partition, result) -> timestamps.put(partition, result.timestamp()));
        return timestamps;
    }

    private static Map<TopicPartition, Long> getConsumerGroupOffsets(AdminClient adminClient, String consumerGroup) throws ExecutionException, InterruptedException {
        // Fetch committed offsets for the consumer group
        Map<TopicPartition, OffsetAndMetadata> groupOffsets = adminClient.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata()
                .get();

        Map<TopicPartition, Long> committedOffsets = new HashMap<>();
        for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : groupOffsets.entrySet()) {
            committedOffsets.put(entry.getKey(), entry.getValue().offset());
        }
        return committedOffsets;
    }

    private static Map<TopicPartition, Long> getOffsetsAndTimestampsForOffset(AdminClient adminClient, List<TopicPartition> topicPartitions, Map<TopicPartition, Long> offsets) throws ExecutionException, InterruptedException {
        // Fetch timestamps for specific offsets
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (TopicPartition partition : topicPartitions) {
            long offset = offsets.getOrDefault(partition, 0L);
            request.put(partition, OffsetSpec.forTimestamp(offset));
        }

        Map<TopicPartition, Long> timestamps = new HashMap<>();
        adminClient.listOffsets(request).all().get()
                .forEach((partition, result) -> timestamps.put(partition, result.timestamp()));
        return timestamps;
    }
}
