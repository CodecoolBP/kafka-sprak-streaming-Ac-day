package com.kafka.spark.mortoff.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumer {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProperties.Producer.class);

    @KafkaListener(topics = "ganuxmx5-default", groupId = "group-id")
    public void consume(String message) {
        LOGGER.info(String.format("$$ -> Consumed message --> %s", message));
    }

}
