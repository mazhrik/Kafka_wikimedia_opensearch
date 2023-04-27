package consumer;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
public class opensearch {

    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String,String> CreateKafkaConsumer(){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","127.0.0.1:9092");

        String group_id = "recentchangeconsumer";


        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",group_id );
        properties.setProperty("ato.offset.reset","earliest");
        return new KafkaConsumer<>(properties);


    }
    private static String extract(String json){
        return JsonParser.parseString(json).getAsJsonObject().get("meta").getAsJsonObject().get("id").getAsString();

    }
    public static void main(String[] args) throws IOException {

        RestHighLevelClient openSearchClient = createOpenSearchClient();
        Logger log = LoggerFactory.getLogger(opensearch.class.getName());
        KafkaConsumer<String,String> consumer=CreateKafkaConsumer();

        try(openSearchClient;consumer) {
            boolean indexexists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexexists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                log.info("Index created ");
            } else {
                log.info("exists");

            }




            consumer.subscribe(Arrays.asList("wikimedia.recentchange"));

            while(true){
                ConsumerRecords<String,String> records= consumer.poll(Duration.ofMinutes(5));
                BulkRequest bulkRequest= new BulkRequest();
                log.info("records recieved");
                for (ConsumerRecord<String,String> record :records){
                    //            stretagy 1
//                    String id = record.partition()+ "-"+record.topic()+"-"+record.offset();
//                    stretagy 2

                    try {
                        String id = extract(record.value());
                        IndexRequest indexRequest = new IndexRequest("wikimedia").source(record.value(), XContentType.JSON).id(id);
//                        openSearchClient.index(indexRequest, RequestOptions.DEFAULT);
                        log.info("inserted");
                        bulkRequest.add(indexRequest);
                    } catch (Exception e) {

                    }

                }
                if (bulkRequest.numberOfActions()> 0) {
                    BulkResponse response = openSearchClient.bulk(bulkRequest, RequestOptions.DEFAULT);
                    log.info("bulke done");

                    Thread.sleep(1000);
                }

            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
//        openSearchClient.close();


    }

}
