package org.ntg;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class ProducerDemoWithKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("Kafka producer");

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        ProducerRecord<String, String> producerRecord;

        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            producerRecord = new ProducerRecord<>("ntg", String.valueOf(random.nextInt(0,2)), String.format("hello %d", i));
            ProducerRecord<String, String> finalProducerRecord = producerRecord;
            producer.send(producerRecord, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info(String.format("\nTopic: %S \nPartition: %s \nKey: %s \nValue: %s \nOffset: %s \nTimeStamp: %s\n",
                                metadata.topic(), finalProducerRecord.key(), finalProducerRecord.value(), metadata.partition(), metadata.offset(), metadata.timestamp()));
                    } else {
                        log.error("error producing " + exception);
                    }
                }
            });
        }

        producer.flush();
        producer.close();
    }
}