package com.kafka.spark.mortoff.kafka;

import com.kafka.spark.mortoff.kafka.service.SparkStreaming;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;


@SpringBootApplication
public class KafkaApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(KafkaApplication.class, args);
        try {
            new SparkStreaming().startSpark();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}