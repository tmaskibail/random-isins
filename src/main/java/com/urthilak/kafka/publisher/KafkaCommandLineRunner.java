package com.urthilak.kafka.publisher;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

@Component
public class KafkaCommandLineRunner implements CommandLineRunner {
    static final Logger LOGGER = LoggerFactory.getLogger(KafkaCommandLineRunner.class);

    @Autowired
    private KafkaPublisher kafkaPublisher;

    @Override
    public void run(String... args) throws Exception {
        LOGGER.info("Running the Kafka publisher");

        List<String> isins = readIsins();
        LOGGER.info("Read {} isins", isins.size());

        int i = 1;
        boolean iterate = true;
        while (iterate) {
            Random random = new Random();
            String msg = isins.get(random.nextInt(isins.size()));
            kafkaPublisher.sendMessage(String.valueOf(i), msg);

            LOGGER.info("Published ISIN : " + msg);

            Thread.sleep(100);
            if (i == (Integer.MAX_VALUE - 1)) {
                iterate = false;
            } else {
                i = i + 1;
            }
        }
    }

    private List<String> readIsins() {
        List<String> isins = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(KafkaCommandLineRunner.class.getClassLoader().getResource("ISIN.txt").getFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                isins.add(line);
            }
        } catch (IOException e) {
            LOGGER.error("IO error", e);
        }
        return isins;
    }
}
