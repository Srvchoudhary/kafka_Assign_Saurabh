import java.util.Properties;

import com.sun.xml.internal.ws.policy.privateutil.PolicyUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
public class Producer {
    public static void main(String[] args) throws Exception{

        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");

        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        String topicname = "student";
        KafkaProducer kafkaProducer = new KafkaProducer(props);

        ProducerRecord producerRecord =new ProducerRecord(topicname,"{\"id\":\"1\",\"name\":\"saurabh\",\"age\":\"24\",\"course\":\"BTech.\"}");
        kafkaProducer.send(producerRecord);
        System.out.println("Message sent successfully");
        kafkaProducer.close();

    }
}