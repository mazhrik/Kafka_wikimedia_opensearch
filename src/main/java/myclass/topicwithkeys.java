package myclass;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class topicwithkeys {
        private static final Logger log =LoggerFactory.getLogger(topicwithkeys.class.getSimpleName());
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
        properties.setProperty("batch.size","400");

//creating producer
        KafkaProducer<String,String> producer = new KafkaProducer<>(properties);
//        creating the producer record

        for (int j =0 ; j<2;j++) {
            int i;
            for (i = 0; i < 30; i++) {
                String topic = "first_topic";
                String key = "id_" + i;
                String value = "hellowwwmuneeb" + i;

                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);


                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
//                executed everytime a record is successfully sent or an exception is thrown
                        if (e == null) {
                            log.info("key \n" + "topic " + key + "\n" + "partition " + metadata.partition() + "\n" + "offset " + metadata.offset() + "\n" + "timestamp " + metadata.timestamp() + "\n");
                        } else {
                            log.error("error", e);
                        }
                    }
                });
            }
        }
//      flush and close
        producer.close();

    }
}
