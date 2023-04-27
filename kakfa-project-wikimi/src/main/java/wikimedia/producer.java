package wikimedia;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventHandler;
import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import wikimedia.wikimedia.chnagehandler;

public class producer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrap = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrap);

        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        String topic = "wikimedia.recentchange";

        EventHandler eventHandler = new chnagehandler(producer,topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler,URI.create(url));
        EventSource eventSource=builder.build();

        eventSource.start();
        TimeUnit.MINUTES.sleep(10);


    }


}
