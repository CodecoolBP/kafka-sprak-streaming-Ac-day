package com.kafka.spark.mortoff.kafka.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkStreaming {

    public void startSpark() throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put("bootstrap.servers", "ark-01.srvs.cloudkafka.com:9094,ark-02.srvs.cloudkafka.com:9094,ark-03" +
                ".srvs.cloudkafka.com:9094");
        kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
        kafkaParams.put("group.id", "group-id");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        kafkaParams.put("security.protocol", "SASL_SSL");
        kafkaParams.put("sasl.mechanism", "SCRAM-SHA-256");
        kafkaParams.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                "username=\"ganuxmx5\" password=\"owQzE9CxXb0whZjMUW-Du6FmMAbQAo4P\";");
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount");

        Collection<String> topics = Arrays.asList("ganuxmx5-default");
        JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(1));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );
//        JavaDStream<String> mapped = stream.map(a -> {
//            String string = a.value();
//            System.out.println(string);
//            return string;
//        });

//        mapped.foreachRDD(rdd -> {
//            System.out.println(rdd.toString());
//        });

        stream.print();
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
