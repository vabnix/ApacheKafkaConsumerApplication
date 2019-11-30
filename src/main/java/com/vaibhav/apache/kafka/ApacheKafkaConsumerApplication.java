package com.vaibhav.apache.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@SpringBootApplication
public class ApacheKafkaConsumerApplication {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ApacheKafkaConsumerApplication.class);
        String bootstrapServer = "127.0.0.1:9092";
        String groupId = "my-first-consumer-app";
        String topic = "first_topic";

        //Creating Consumer Properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //Create Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);

        //Subscribe Consumer to topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //poll for new record
        while (true){
            ConsumerRecords<String, String>  consumerRecords = kafkaConsumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String, String> record : consumerRecords){
                logger.info("--------------------------");
                logger.info("Key: "+ record.key());
                logger.info("Value: "+ record.value());
                logger.info("Partition: " + record.partition() + " Offsets: " + record.offset());
                logger.info("--------------------------");
            }
        }
    }

}
