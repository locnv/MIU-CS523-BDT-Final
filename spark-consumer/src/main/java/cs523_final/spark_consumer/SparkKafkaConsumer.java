package cs523_final.spark_consumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class SparkKafkaConsumer {

	public static void main(String[] args) throws IOException {

		System.out.println("Spark Streaming started now .....");

		SparkConf conf = new SparkConf().setAppName("kafka-sandbox").setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(20000));

		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", "localhost:9092");
		String topicArray[] = { "stock", "crypto" };
		Set<String> topics = new HashSet<>(Arrays.asList(topicArray));


		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils
			.createDirectStream(
				ssc,
				String.class,
				String.class,
				StringDecoder.class,
				StringDecoder.class,
				kafkaParams,
				topics
			);

		KafkaRecord allStockRecord = new KafkaRecord();
		KafkaRecord allCryptoRecord = new KafkaRecord();

		directKafkaStream
				.foreachRDD(rdd -> {
					System.out.println("New data arrived  " + rdd.count() + " Records");
					if (rdd.count() > 0) {
						rdd.collect().forEach(rawRecord -> {
							String record = rawRecord._2();
							String[] tokens = record.split(",", 2);
							String topicName = tokens[0];
							
							if(topicName.equalsIgnoreCase("stock")) {
								allStockRecord.addRecord(tokens[1]);
								allStockRecord.setTopicName(topicName);
							} else if(topicName.equalsIgnoreCase("crypto")) {
								allCryptoRecord.addRecord(tokens[1]);
								allCryptoRecord.setTopicName(topicName);
							}
							
							System.out.println(rawRecord._2);
						});

						if(allStockRecord.getAllRecord().size() > 0) {
							JavaRDD<String> dstRDD = sc.parallelize(allStockRecord.getAllRecord());
							storeRDD(allStockRecord.getTopicName(), dstRDD);
							allStockRecord.getAllRecord().clear();
						}
						
						if(allCryptoRecord.getAllRecord().size() > 0) {
							JavaRDD<String> dstRDD1 = sc.parallelize(allCryptoRecord.getAllRecord());
							storeRDD(allCryptoRecord.getTopicName(), dstRDD1);
						}
					}
				});

		ssc.start();
		ssc.awaitTermination();
	}

	static void storeRDD(String topicName, JavaRDD<?> rdd) {
		String outPath = 
				"hdfs://quickstart.cloudera:8020/user/cloudera/cs523/final/"
				+ topicName;

		rdd.saveAsTextFile(outPath);
	}

}