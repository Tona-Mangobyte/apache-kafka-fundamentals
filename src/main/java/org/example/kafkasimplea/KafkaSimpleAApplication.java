package org.example.kafkasimplea;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class KafkaSimpleAApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaSimpleAApplication.class, args);
        // specify connection properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "MyFirstConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // receive messages that were sent before the consumer started
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // create the consumer using props.
        try (final Consumer<Long, String> consumer = new KafkaConsumer<>(props)) {
            // subscribe to the topic.
            final String topic = "my-first-topic";
            consumer.subscribe(Arrays.asList(topic));
            // poll messages from the topic and print them to the console
            consumer
                    .poll(Duration.ofMinutes(1))
                    .forEach(System.out::println);
        }
    }

}
