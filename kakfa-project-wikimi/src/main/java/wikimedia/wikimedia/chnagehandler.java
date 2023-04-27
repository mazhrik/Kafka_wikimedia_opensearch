package wikimedia.wikimedia;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.net.URI;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class chnagehandler implements EventHandler {
    String topic;
    KafkaProducer<String,String> producer;
    public static final Logger log =LoggerFactory.getLogger(chnagehandler.class.getName());

    public chnagehandler (KafkaProducer<String,String> producer,String topic){

        this.producer=producer;
        this.topic=topic;
    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {
        producer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(topic,messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.info("_____________________________________________________________________________________");

    }
}
