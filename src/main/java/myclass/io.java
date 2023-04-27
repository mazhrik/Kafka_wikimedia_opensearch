package myclass;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.datatransfer.StringSelection;
import java.util.Properties;

public class io {
        private static final Logger log =LoggerFactory.getLogger(io.class.getSimpleName());
    public static void main(String[] args) {
        log.info("i am a producer!");
        Properties properties = new Properties();
//        connect to localhost
//        properties.setProperty("bootstrap.servers","127.0.0.1:9092");
//        connect to coducter playground
        properties.setProperty("bootstrap.servers","cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol","SASL_SSL");
        properties.setProperty("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"62umnhZa8W6ZO5Fl6EKsbp\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiI2MnVtbmhaYThXNlpPNUZsNkVLc2JwIiwib3JnYW5pemF0aW9uSWQiOjcyMjE5LCJ1c2VySWQiOjgzODQxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiIzZGRmYjZkZi0wZDdlLTQ0NGEtOWY3Yy1hMWU0YjBlN2U2YjkifX0.ODWHayJGKBWRdwZy7INlnc0JuKjD0d6_HDl1NxG4gGg\";");
        properties.setProperty("sasl.mechanism","PLAIN");





//        set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer",StringSerializer.class.getName());

//creating producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
//        creating the producer record
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>("first_topic","helo world muneeb");

//        send data
        producer.send(producerRecord);
//      flush and close
        producer.close();

    }
}
