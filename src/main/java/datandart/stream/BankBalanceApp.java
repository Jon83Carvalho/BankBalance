package datandart.stream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;


import java.time.Instant;
import java.util.Arrays;
import java.util.Properties;

public class BankBalanceApp {
    public static void main(String[] args) {
        Properties config=new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"bank-balance-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"a83d7cfeae6d94812ae052860f089bf3-835140697.us-east-2.elb.amazonaws.com:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG,"0");

        // Exactly once processing!!
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);

        // json Serde

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer()  ;
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer,jsonDeserializer);

        StreamsBuilder builder=new StreamsBuilder();

        //1 - Stream from Kafka
        KStream<String,JsonNode> balanceRecord=builder.stream("balance-input", Consumed.with(Serdes.String(), jsonSerde));

        //create json object
        ObjectNode initialBalance= JsonNodeFactory.instance.objectNode();
        initialBalance.put("count",0);
        initialBalance.put("balance",0);
        initialBalance.put("time", Instant.ofEpochMilli(0L).toString());



        KTable<String,JsonNode> balancedata=balanceRecord
                //1 - Agrupanddo os valroes
                .groupByKey(Serdes.String(),jsonSerde)
                .aggregate(
                        ()->initialBalance,
                        (key,transaction,balance)->newBalance(transaction,balance),
                        jsonSerde,
                        "bank-balance-agg"
                );

        balancedata.toStream().to("balance-output", Produced.with(Serdes.String(),jsonSerde));

        KafkaStreams streams=new KafkaStreams(builder.build(),config);

        streams.start();

        //print the topology
        System.out.println(streams.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));


    }
    private static JsonNode newBalance (JsonNode transaction,JsonNode balance) {
        //create new balance json object
        ObjectNode newBalance=JsonNodeFactory.instance.objectNode();
        newBalance.put("count",balance.get("count").asInt()+1);
        newBalance.put("balance",balance.get("balance").asInt()+transaction.get("amount").isInt());

        Long balanceEpoch=Instant.parse(balance.get("time").asText()).toEpochMilli();
        Long transactionEpoch=Instant.parse(transaction.get("time").asText()).toEpochMilli();
        Instant newBalanceInstant=Instant.ofEpochMilli(Math.max(balanceEpoch,transactionEpoch));
        newBalance.put("time",newBalanceInstant.toString());

    }
}
