package com.urthilak.kafka.publisher;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisher {

    public static final String TOPIC_NAME = "quotes";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String key, String msg) {
        kafkaTemplate.send(TOPIC_NAME, key, msg);
    }

}
