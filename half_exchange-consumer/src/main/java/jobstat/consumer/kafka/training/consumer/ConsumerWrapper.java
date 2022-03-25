package jobstat.consumer.kafka.training.consumer;

import com.mycorp.mynamespace.JobStat;
import jobstat.consumer.kafka.training.manager.JobStatManager;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.Properties;

@Service
public class ConsumerWrapper {

    @Autowired
    JobStatManager jobStatManager;

    public void startConsuming(Properties props) throws Exception {
        Consumer<String, JobStat> consumer = new KafkaConsumer<>(props);

        if(props.get("app_topic") == null) {
            throw new Exception("Fill props with 'app_topic'");
        }

        consumer.subscribe(Collections.singletonList(props.getProperty("app_topic")));
        System.err.println("--Subscribed");

        while(true) {

            ConsumerRecords<String, JobStat> records = consumer.poll(1000);
            System.err.println("-- something polled size: " + records.count());

            records.forEach(record -> jobStatManager.insertOrUpdate(record.value()));

        }
    }

}
