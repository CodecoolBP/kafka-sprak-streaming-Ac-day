package com.kafka.spark.mortoff.kafka.service;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Service
public class KafkaConsumer {

    private SparkConf sparkConf = new SparkConf();

    private JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));

    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> params = new HashMap<>();
        params.put("bootstrap.servers", "ark-01.srvs.cloudkafka.com:9094, ark-02.srvs.cloudkafka.com:9094, ark-03.srvs.cloudkafka.com:9094");
        params.put("key.deserializer", StringDeserializer .class);
        params.put("value.deserializer", StringDeserializer.class);
        params.put("group.id", "use_a_separate_group_id_for_each_stream");
        params.put("auto.offset.reset", "latest");
        params.put("enable.auto.commit", false);
        return consumerFactory();
    }


    private Collection<String> topics = Arrays.asList("messages");


//    JavaReceiverInputDStream<String> lines = streamingContext.receiverStream();

//    JavaDStream<String> words = lines.flatMap();

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaProperties.Producer.class);

    @KafkaListener(topics = "ganuxmx5-default", groupId = "group-id")
    public void consume(String message) {
        LOGGER.info(String.format("$$ -> Consumed message --> %s", message));
    }

}
