package org.ntg;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithCooperativeBalance {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithCooperativeBalance.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kafka2");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        properties.setProperty(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        String topic = "ntg";
        consumer.subscribe(List.of(topic));

        Thread currentThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.info("Detected a shutdown. Let's call consumer wakeup");
                consumer.wakeup();

                try {
                    currentThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            while (true) {
                ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));
                Iterable<ConsumerRecord<String, String>> records = consumerRecords.records(topic);
                for (ConsumerRecord<String, String> record: records) {
                    log.info(String.format("Partition: %s Key: %s Value: %s Offset: %s TimeStamp: %s\n",
                            record.partition(),record.key(), record.value(), record.offset(), record.timestamp()));
                }
                //Thread.sleep(10000);
            }
        } catch (WakeupException e) {
            log.info("Wake up exception");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close();
            log.info("The consumer is gracefully closed");
        }
    }
}
