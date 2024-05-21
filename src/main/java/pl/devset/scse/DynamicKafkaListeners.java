package pl.devset.scse;


import jakarta.annotation.PostConstruct;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

@Configuration
public class DynamicKafkaListeners {

    @Value("${spring.kafka.partition:1}")
    private int partitionCount;

    @Value("${spring.kafka.consumer.topic-name}")
    private String topicName;

    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;
    private final EventServices services;

    public DynamicKafkaListeners(ConcurrentKafkaListenerContainerFactory<String, String> factory, EventServices services) {
        this.factory = factory;
        this.services = services;
    }

    private final List<ConcurrentMessageListenerContainer<String, String>> containers = new ArrayList<>();

    @PostConstruct
    public void createListeners() {
        for (int i = 0; i < partitionCount; i++) {
            containers.add(createContainer(i));
        }
    }

    private ConcurrentMessageListenerContainer<String, String> createContainer(int partition) {
        ContainerProperties containerProps = new ContainerProperties(new TopicPartitionOffset(topicName, partition));
        containerProps.setGroupId("devset");

        containerProps.setMessageListener(new MessageListener<String, String>() {
            @Override
            public void onMessage(ConsumerRecord<String, String> message) {
                var device = extractHeaderValue(message, "userId");

                services.processEvent(device, message.value());
            }
        });

        ConcurrentMessageListenerContainer<String, String> container = new ConcurrentMessageListenerContainer<>(factory.getConsumerFactory(), containerProps);
        container.setConcurrency(1);
        container.start();
        return container;
    }

    private String extractHeaderValue(ConsumerRecord<String, String> record, String headerKey) {
        for (Header header : record.headers().headers(headerKey)) {
            return new String(header.value(), StandardCharsets.UTF_8);
        }
        return null;
    }
}
