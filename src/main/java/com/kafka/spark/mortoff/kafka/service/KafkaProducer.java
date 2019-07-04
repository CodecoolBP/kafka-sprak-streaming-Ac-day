/*package com.kafka.spark.mortoff.kafka.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
    private static final String TOPIC = "ganuxmx5-default";

    public void sendMessage(String message) {
        LOGGER.info(String.format("$$ -> Producing message --> %s", message));
        this.kafkaTemplate.send(TOPIC, message);
    }

}
*/