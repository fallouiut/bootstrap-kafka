package jobstat.consumer.kafka.training;

import com.mycorp.mynamespace.JobStat;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import jobstat.consumer.kafka.training.consumer.ConsumerWrapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.Scheduled;

import java.util.Collections;
import java.util.Properties;

@SpringBootApplication
public class Application {

	final static String TOPIC = "s360-half_exchange";

	@Autowired
	ConsumerWrapper consumerWrapper;

	public static void main(String[] args) {
		SpringApplication.run(Application.class, args);
	}

	@Scheduled(fixedRate=60*60*1000)
	public void scheduleProducer() throws Exception {
		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "2000");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG  , StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
		props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);
		props.put("app_topic", "person-job-stat");
		props.put("auto.register.schemas", false);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
		props.put("schema.registry.url", "http://localhost:8081");

		//////////////////////////////////////////////////////////////////////////////

		Consumer<String, JobStat> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Collections.singletonList(props.getProperty(TOPIC)));

		System.err.println("--Subscribed");

		while(true) {

			ConsumerRecords<String, JobStat> records = consumer.poll(1000);
			System.err.println("-- something polled size: " + records.count());

			records.forEach(record -> {
				System.err.println(record.value());
			});

		}

	}

}
