package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import ru.mai.lessons.rpks.KafkaWriter;
import ru.mai.lessons.rpks.model.Message;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

@Slf4j
public class KafkaWriterImpl implements KafkaWriter {
    private final Config config;
    private final String topic;
    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaWriterImpl(Config config) {
        this.config = config;
        this.topic = config.getString("kafka.producer.topic");
        this.kafkaProducer = new KafkaProducer<>(
                kafkaProducerConfig(),
                new StringSerializer(),
                new StringSerializer());
        log.info("Created Kafka Producer for topic: {}", topic);
    }

    private Map<String, Object> kafkaProducerConfig() {
        return config.getConfig("kafka.producer")
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().unwrapped()));
    }

    @Override
    public void processing(Message message) {
        log.debug("Sending message: {}", message.getValue());
        kafkaProducer.send(new ProducerRecord<>(topic, message.getValue()), (metadata, exception) -> {
            if (exception == null) {
                log.info("Message sent successfully to topic {} partition {} with offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            } else {
                log.error("Error sending message to topic {}: {}", metadata.topic(), exception.getMessage(), exception);
            }
        });
    }


}