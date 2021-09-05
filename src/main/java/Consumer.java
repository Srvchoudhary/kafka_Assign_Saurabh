import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Consumer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();

        String topicname = "student";
        String consumergroup = "studentgroup";
        props.put("bootstrap.servers","localhost:9092");
        props.put("group.id",consumergroup);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");


        KafkaConsumer<String,String> consumer= new KafkaConsumer<String,String>(props);

        consumer.subscribe(Arrays.asList(topicname));

        // FileWriter object for writing the consumed message into text file

        FileWriter fw = new FileWriter("/home/knoldus/Downloads/kafka_Assign_Saurabh/src/main/saurabh.txt",true);


        while(true){
            ConsumerRecords<String,String> records= consumer.poll(1000);
            for(ConsumerRecord<String,String> record: records){
                System.out.println(record.value().toString());
                fw.write(record.value().toString());   // writing into the file
                fw.close();
            }
        }
    }
}