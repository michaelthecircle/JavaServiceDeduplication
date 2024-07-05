package ru.mai.lessons.rpks.impl;

import com.typesafe.config.Config;
import ru.mai.lessons.rpks.KafkaReader;
import ru.mai.lessons.rpks.Service;

public class ServiceDeduplication implements Service {
    @Override
    public void start(Config config) {
        KafkaReader kafkaReader = new KafkaReaderImpl(config);
        kafkaReader.processing();
    }
}
