package myclass;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class consumerdemo2_consumergroup {
        private static final Logger log =LoggerFactory.getLogger(consumerdemo2_consumergroup.class.getSimpleName());
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

        String group_id = "java-consumer-group";
        String topic = "first_topic";



//        set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",group_id );
        properties.setProperty("ato.offset.reset","earliest");


        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
//        get the refrence of the main thread . refrence to the thread which is running our program
        final Thread mainThread = Thread.currentThread();

//                shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("wakup method");
                consumer.wakeup();
                try{
                    mainThread.join();

                } catch (InterruptedException e){
                    e.printStackTrace();
                }


            }


        });
        try {
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                log.info("polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
                log.info("polling_______");

                for (ConsumerRecord<String, String> record : records) {
                    log.info("___________________________________");
                    log.info("key " + record.key() + "value " + record.value() + "\n");
                    log.info("partition  " + record.partition() + "offset " + record.offset() + "\n");


                }

            }
        }catch (WakeupException e ){
            log.info("wakingup");
        }finally {
            consumer.close();
            log.info("closed");
        }

    }
}
