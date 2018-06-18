package shangbo.kafka.example4;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class App {
	public static void main(String[] args) {
		Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("group.id", "Consumer1"); // 消费者组的标识
	     props.put("enable.auto.commit", "true"); // 收到消息后，自动提交 offset
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("auto.offset.reset", "earliest"); // 如果消费者组第一次接收消息，从哪里开始呢？earliest：最早，latest：最新
	     
	     // 创建 Consumer
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
	     
	     // 从 topic0 接收数据
	     consumer.subscribe(Arrays.asList("topic0"));
	     
	     // 也可以从指定的 topic/partition 接收数据
//	     TopicPartition partition0 = new TopicPartition("topic0", 0);
//	     consumer.assign(Arrays.asList(partition0));
	     
	     while (true) {
	    	 // 接收消息
	         ConsumerRecords<String, String> records = consumer.poll(100);
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	     }
	}
}
