package com.kafka.spark.mortoff.kafka.controller;

import com.kafka.spark.mortoff.kafka.model.User;
import com.kafka.spark.mortoff.kafka.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.stereotype.Controller;

import java.io.Serializable;


@Controller
public class KafkaController implements Serializable {

    @Autowired
    private UserService userService;

    @MessageMapping("/userlist")
    @SendTo("/users/user")
    public static User user(User user) throws Exception {
        Thread.sleep(1000);
        return new User(user.getName(), user.getAge());
    }

}
