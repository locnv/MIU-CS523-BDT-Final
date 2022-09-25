package cs523_final.kafka_producer;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class CSVKafkaProducer {

	private static String KafkaBrokerEndpoint = "localhost:9092";
	private static String KafkaTopicStock = "stock";
	private static String KafkaTopicCrypto = "crypto";
	
	private Producer<String, String> ProducerProperties() {
		Properties properties = new Properties();
		properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				KafkaBrokerEndpoint);
		properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
		properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());

		return new KafkaProducer<String, String>(properties);
	}

	public static void main(String[] args) throws URISyntaxException {
		String stockFileNames[] = {
				"AAPL.csv",
				"AMZN.csv",
				"META.csv",
				"NFLX.csv",
				"TSLA.csv"
				};
		String cryptoFileNames[] = { "BTC-USD.csv" };

		CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();

		for (String fileName : stockFileNames) {
			kafkaProducer.PublishMessages(fileName, KafkaTopicStock);
		}
		
		for (String fileName : cryptoFileNames) {
			kafkaProducer.PublishMessages(fileName, KafkaTopicCrypto);
		}

		System.out.println("Producing job completed");
	}

	@SuppressWarnings("resource")
	private void PublishMessages(String fileName, String topicName) throws URISyntaxException {

		final Producer<String, String> csvProducer = ProducerProperties();
		final String stock = fileName.replaceFirst("[.][^.]+$", "");
		try {
			URI uri = getClass().getClassLoader().getResource(fileName).toURI();
			Stream<String> FileStream = Files.lines(Paths.get(uri));

			FileStream
			.skip(1)
			.forEach(line -> {
				String modifiedLine = topicName + "," + stock + "," + line;
				// System.out.println(modifiedLine);

				final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
						topicName, UUID.randomUUID().toString(), modifiedLine);

				csvProducer.send(
					csvRecord,
					(metadata, exception) -> {
						if (metadata != null) {
							System.out.println("CsvData: -> " + csvRecord.key() + " | " + csvRecord.value());
						} else {
							System.out.println("Error Sending Csv Record -> " + csvRecord.value());
						}
					});

				try {
					Thread.sleep(20);
				} catch (Exception e) {
				}
			});

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}