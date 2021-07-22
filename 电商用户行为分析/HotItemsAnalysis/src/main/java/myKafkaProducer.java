import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.Properties;

/**
 * @Description: java类作用描述
 * @Author: Xiaoyuyu
 * @CreateDate: 2021/7/22 5:23 下午
 */
public class myKafkaProducer {

    public static void main(String[] args) throws IOException {
        write2kafka("hotItem");
    }
    public static void write2kafka(String topic) throws IOException {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","192.168.166.200:9092");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String,String>(properties);
        InputStreamReader read = new InputStreamReader(new FileInputStream("data/UserBehavior.csv"));
        BufferedReader bufferedReader = new BufferedReader(read);
        while (true) {
            String s = bufferedReader.readLine();
            if(s!=null) {
                producer.send(new ProducerRecord<String, String>(topic,s));
            }
        }
    }
}
