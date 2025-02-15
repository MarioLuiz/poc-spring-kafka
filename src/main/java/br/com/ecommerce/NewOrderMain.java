
package br.com.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var producer = new KafkaProducer<String, String>(properties());
        var value = "70707, 2315897,1321321789";
        var CostumerEmail = "costumer@gmail.com";
        var email = "Thank you for your order! We are processing your order!";

        var record = new ProducerRecord<>("ECOMMERCE_NEW_ORDER", value, value);
        var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", CostumerEmail, email);

        Callback callback = (metadata, exception) -> {
            if (exception != null) {
                exception.printStackTrace();
                return;
            }
            System.out.println("Sucesso enviando " + metadata.topic() + "::: partition: " + metadata.partition() + ", offset: " + metadata.offset());
        };

        producer.send(record, callback).get();
        producer.send(emailRecord, callback).get();
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

}
