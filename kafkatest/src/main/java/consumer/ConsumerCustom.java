package consumer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerCustom {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers",
                "10.3.68.20:9092,"
                + "10.3.68.21:9092,"
                + "10.3.68.23:9092,"
                + "10.3.68.26:9092,"
                + "10.3.68.28:9092,"
                + "10.3.68.32:9092,"
                + "10.3.68.47:9092,"
                + "10.3.68.48:9092,"
                + "10.3.68.50:9092,"
                + "10.3.68.52:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // The extra properties we specify compared to when creating
        // a Kakfa producer
        props.put("auto.offset.reset", "earliest");
        props.put("group.id", "NguyenltConsumers");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        ArrayList<String> a = new ArrayList<String>();
        a.add("rt-adn-sp");
        consumer.subscribe(a);
        try {
            // We have shown an infinite loop for instructional purposes
            while (true) {
                Duration oneSecond = Duration.ofMillis(1000);

                // Poll the topic for new data and block for one second if new
                // data isn't available.
                ConsumerRecords<String, String> records = consumer.poll(oneSecond);

                // Loop through all the records received
                for (ConsumerRecord<String, String> record : records) {

                    String topic = record.topic();
                    int partition = record.partition();
                    long recordOffset = record.offset();
                    String key = record.key();
                    String value = record.value();

                    System.out.println("topic: %s" + topic + "\n" + "partition: %s" + partition + "\n"
                            + "recordOffset: %s" + recordOffset + "\n" + "key: %s" + key + "\n" + "value: %s" + value);
                }
            }
        } finally {
            // Remember to close the consumer. If the consumer gracefully exits
            // the consumer group, the coordinator can trigger a rebalance immediately
            // and doesn't need to wait and detect that the consumer has left.
            consumer.close();
        }
    }
}