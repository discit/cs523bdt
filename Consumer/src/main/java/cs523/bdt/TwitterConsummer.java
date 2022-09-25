package cs523.bdt;


import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

public class TwitterConsummer {

	public static void main(String[] args)  {

		String zkQuorum = "localhost:2181"; //zkQuorum  Zookeeper quorum
		
		SparkConf sparkConf = new SparkConf().setAppName("Twitter").setMaster("local[2]");
		JavaSparkContext ctx = JavaSparkContext.fromSparkContext(SparkContext.getOrCreate(sparkConf));
		JavaStreamingContext jssc = new JavaStreamingContext(ctx, Durations.milliseconds(10000));
		jssc.sparkContext().setLogLevel("ERROR");
		
		// jssc.checkpoint(checkpointDir);
		// <list columns,values>

//		Map<String, String> kafkaParams = new HashMap<>();
//		kafkaParams.put("zookeeper.connect", "localhost" + ":2181");
//		kafkaParams.put("group.id", "group1");
//		kafkaParams.put("auto.offset.reset", "smallest");
//		kafkaParams.put("metadata.broker.list", "locahost" + ":9092");
//		kafkaParams.put("bootstrap.servers", "locahost" + ":9092");
		Map<String, Integer> topics1 = new HashMap<String, Integer>();
		topics1.put("bigdata", 1);
		JavaPairReceiverInputDStream<String, String> kafkaStream = KafkaUtils.createStream(jssc, zkQuorum, "twitter", topics1); //Create an input stream that pulls messages from Kafka Brokers

		JavaDStream<Tuple2<String, String>> text = kafkaStream.toJavaDStream();
		text.foreachRDD((rdd, time) -> {
			
			JavaRDD<String> plainRDD = rdd.coalesce(1).map(tuple -> (String) tuple._2()).map(s -> s.replaceAll("[^\\x00-\\x7E]|\\r|\\n|\\r\\n", " "));
			//System.out.println("FFFFFFFFFFF" + plainRDD.toString());
			sendDataToHDFS(plainRDD);
		});
		
		text.print();
		
		jssc.start(); //Start the execution of the streams.
		jssc.awaitTermination();
	}
	
	
	public static void sendDataToHDFS(JavaRDD<String> plainRDD){
		
			plainRDD.saveAsTextFile("hdfs://quickstart.cloudera:8020/user/cloudera/temp_tweets/" + System.currentTimeMillis());
		
	
	}
	
}