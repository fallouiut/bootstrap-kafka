package service;

import com.mycorp.mynamespace.PersonJob;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

public class KafkaProducerWrapper {

    private Properties props = new Properties();
    private KafkaProducer<String, PersonJob> producer = null;

    public KafkaProducerWrapper setBootStrapServers(String bootStrapServers) {
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        return this;
    }

    public KafkaProducerWrapper setKeyValueSerializers(String keySe, String valSe) {

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG  , keySe);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valSe);
        return this;
    }

    public KafkaProducerWrapper setSchemaRegistryUrl(String url) {
        props.put("schema.registry.url", url);
        return this;
    }

    public void startProducing() {
        Thread.currentThread().setContextClassLoader(null);

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-boot-producer");
        producer = new KafkaProducer<String, PersonJob>(props);
        System.err.println("Producer starting producing");
    }

    public void publish(String topic, String key, PersonJob value) {
        ProducerRecord<String, PersonJob> record = new ProducerRecord<>(topic, key, value);
        producer.send(record, this::handleAfterRecord);
        producer.flush();
    }

    public void handleAfterRecord(RecordMetadata metadata, Exception e) {
        if(e != null) {
            System.err.println("KafkaProducerWrapper.publish() Exception");
            System.err.println(e.getMessage());
            e.printStackTrace();
        } else {
            System.err.println("One Record sent to topic '" + metadata.topic() + "' at partition " + metadata.partition() + " and offset: " + metadata.offset());
        }
    }

    public void close() {
        producer.close();
    }

}
