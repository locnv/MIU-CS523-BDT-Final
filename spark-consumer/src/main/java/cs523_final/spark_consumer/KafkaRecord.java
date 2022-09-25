package cs523_final.spark_consumer;

import java.util.ArrayList;
import java.util.List;

public class KafkaRecord {
	private List<String> allRecord = new ArrayList<String>();
	private String topicName = null;
	public KafkaRecord() {
		
	}
	
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}
	
	public String getTopicName() {
		return topicName;
	}
	
	public void addRecord(String token) {
		this.allRecord.add(token);
	}
	
	public List<String> getAllRecord() {
		return allRecord;
	}
}
