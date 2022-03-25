package kafka.training.person.producer;

import com.mycorp.mynamespace.PersonJob;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Scheduled;
import service.KafkaProducerWrapper;
import service.PersonJobGenerator;

import javax.annotation.PostConstruct;
import java.util.Properties;

@SpringBootApplication
public class Application {
    
    final static String BOOT_SERVERS = "localhost:9092";
    final static String KEY_SER = StringSerializer.class.getName();//"org.apache.kafka.common.serialization.StringSerializer";
    final static String VAL_SER = KafkaAvroSerializer.class.getName();//"org.apache.kafka.common.serialization.StringSerializer";
    final static String REGISTRY = "http://localhost:8081";
    final static String TOPIC = "s360-half_exchange";
    final static String KEY = "half-exchange-key";

    private Properties props = new Properties();
    private KafkaProducer<String, Object> producer = null;

    @PostConstruct
    public void scheduleProducer() {

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOT_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG  , KEY_SER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SER);
        props.put("schema.registry.url", REGISTRY);
        props.put("auto.register.schemas", false);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "spring-boot-producer");

        producer = new KafkaProducer<String, Object>(props);

        System.err.println("Producer starting producing");
        for(int i = 0; i < 10; ++i) {

            Object value = new String("efgoprjhpergeophg");

            ProducerRecord<String, Object> record = new ProducerRecord<>(TOPIC, KEY, value);
            producer.send(record, this::handleAfterRecord);
            producer.flush();
        }


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

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

}

