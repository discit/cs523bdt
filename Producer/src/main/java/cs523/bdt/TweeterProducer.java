package cs523.bdt;

import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
//import KafkaProducer packages
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.google.gson.Gson;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import cs523.bdt.callback.BasicCallback;
import cs523.bdt.config.KafkaConfig;
import cs523.bdt.config.TwitterConfig;
import cs523.bdt.model.Tweet;

public class TweeterProducer{
    
    private Client client;
    private BlockingQueue<String> queue;
    private Gson gson;
    private Callback callback;

	InputStream inputStream;

    
    public TweeterProducer() {
        // Configure authen: OAuth1
        Authentication authentication = new OAuth1(
        		TwitterConfig.CONSUMER_KEY,
        		TwitterConfig.CONSUMER_SECRET,
        		TwitterConfig.ACCESS_TOKEN,
        		TwitterConfig.TOKEN_SECRET);
        
 
        // track the term that you want - here we're tracking #eth.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Collections.singletonList(TwitterConfig.HASHTAG));

        queue = new LinkedBlockingQueue<>(10000);
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .authentication(authentication)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();
        gson = new Gson();
        callback = new BasicCallback();
    }

    private Producer<Long, String> getProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConfig.SERVERS);
        properties.put(ProducerConfig.ACKS_CONFIG, "1");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 500);
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<Long, String>(properties);
    }

    public void run() {
        client.connect();
        //int count = 0;
        try (Producer<Long, String> producer = getProducer()) {
            while (true) {
                Tweet tweet = gson.fromJson(queue.take(), Tweet.class);
                System.out.println("--------------tweet start -------------");
                System.out.println(tweet);
                System.out.println("------------tweet end -------");

                long key = tweet.getId();
                String msg = tweet.toString();
                ProducerRecord<Long, String> record = new ProducerRecord<>(KafkaConfig.TOPIC, key, msg);
                
                producer.send(record, callback);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            client.stop();
        }
    }
    
}

