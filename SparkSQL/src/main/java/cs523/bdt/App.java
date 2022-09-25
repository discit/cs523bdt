package cs523.bdt;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession; 
import org.apache.spark.sql.SparkSession.Builder;

class App {

	public static void main(String[] args) throws Exception  {
		SparkConf conf = new SparkConf().setAppName("StreamingJob").setMaster("local[1]");

		conf.set("spark.sql.warehouse.dir", "hdfs://127.0.0.1:8020/user/hive/warehouse/");
		conf.set("hive.metastore.uris", "thrift://localhost:9083");
		conf.set("hive.exec.scratchdir", "hdfs://127.0.0.1:8020/user/cloudera/temp/");
		
		Builder bd = SparkSession.builder();
		Builder bd1 = bd.config(conf);
		SparkSession ss = bd1.enableHiveSupport().getOrCreate();;				
		//SparkSession ss = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate();
		//ss.sql("show databases").show();
		//ss.sql("use default").show(false);
		ss.sql("show tables").show();

		Scanner kbIn = new Scanner (System.in);

		String decision;
		boolean yn = true;
		while (yn) {
			System.out.println("Do you want to continue? : y or n");
			decision = kbIn.nextLine();
			switch(decision)
			{
			case "n":
				yn = false;
				break;
			case "y":
				
				FileCollector.merge();
				
				//Analysis 1
				ss.sql("DROP TABLE IF EXISTS tb_size_avg").show();
				ss.sql("CREATE TABLE IF NOT EXISTS tb_size_avg(location string, avg string)");
				ss.sql("INSERT INTO tb_size_avg SELECT location, AVG(LENGTH(text)) avg FROM twitterData GROUP BY location");
				Dataset<Row> query = ss.sql("SELECT * FROM tb_size_avg LIMIT 10");
				query.show();
				System.out.println("================Analysis 1 - Average message length per location  ===================");
				
				System.out.println("Do you want to continue, it will run Analysis 2 ? : y or n");
				decision = kbIn.nextLine();
				if(decision.equals("n")) 
					break;

				//Analysis 2
				ss.sql("DROP TABLE IF EXISTS tb_popular_tweets").show();
				ss.sql("CREATE TABLE IF NOT EXISTS tb_popular_tweets(tweetid string, tweettext string, retweet_cnt bigint, fav_cnt bigint, screenname string)");
				ss.sql("INSERT INTO tb_popular_tweets SELECT id, text, CAST(retweetcount AS bigint) retweetcount, CAST(favoritecount AS bigint) favoritecount, screenname "
						+ "FROM twitterData WHERE CAST(followerscount AS bigint) > 300 "
						+ "ORDER BY CAST(followerscount AS bigint)");
				query = ss.sql("SELECT * FROM tb_popular_tweets");
				query.show();
				System.out.println("================Analysis 2 - Filter tweets what has follwers greater than 300 ===================");
			}

		}
	}

}
