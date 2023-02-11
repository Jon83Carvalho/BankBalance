package datandart.stream;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class BankBalanceProducer {
    public static void main(String[] args) {
        Properties config=new Properties();

        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"ad33fd143474544daa4a590ef88b97bf-2129345126.us-east-2.elb.amazonaws.com:9092");
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        config.put(ProducerConfig.ACKS_CONFIG,"all");
        config.put(ProducerConfig.RETRIES_CONFIG,"3");
        config.put(ProducerConfig.LINGER_MS_CONFIG,"1");

        config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        Producer<String,String> producer=new KafkaProducer<>(config);

        int i=0;

        while (true){
            System.out.println("Producer hatch " + i);
            try {
                producer.send(newRandomTransaction("john"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("stephane"));
                Thread.sleep(100);
                producer.send(newRandomTransaction("alice"));
                Thread.sleep(100);
                i+=1;
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }
    private static ProducerRecord<String,String> newRandomTransaction (String name) {
        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        Integer amount= ThreadLocalRandom.current().nextInt(0,100);
        Instant now=Instant.now();
        transaction.put("name",name);
        transaction.put("amount",amount);
        transaction.put("time",now.toString());

        return new ProducerRecord<>("balance-input",name,transaction.toString());

    }
}
