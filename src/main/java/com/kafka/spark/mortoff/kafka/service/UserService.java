package com.kafka.spark.mortoff.kafka.service;

import com.kafka.spark.mortoff.kafka.model.User;
import org.springframework.stereotype.Service;

import java.util.LinkedList;

@Service
public class UserService {
    LinkedList<User> users = new LinkedList<>();

    public void savePerson(User user) {
        users.add(user);
        System.out.println(users);
    }

    public User lastUser() {
        return users.getLast();
    }
}
