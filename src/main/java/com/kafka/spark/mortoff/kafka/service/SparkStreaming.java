package com.kafka.spark.mortoff.kafka.service;

import com.kafka.spark.mortoff.kafka.controller.KafkaController;
import com.kafka.spark.mortoff.kafka.model.User;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;

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

        JavaDStream<User> mapped = stream.map(data -> {
            User user = new User();

            String[] values = data.value().split(",");

            user.setId(Integer.valueOf(values[0]));
            user.setName(values[1]);
            user.setCountry(values[2]);
            user.setAge(Integer.parseInt(values[3]));
//            userService.savePerson(user);
            return user;
        });

        mapped.foreachRDD(rdd -> {

            rdd.foreach(person -> {
//                userService.savePerson(person);
                System.out.println(person.toString());
                KafkaController.user(person);
            });
        });

//        stream.print();
//        mapped.print();

        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
