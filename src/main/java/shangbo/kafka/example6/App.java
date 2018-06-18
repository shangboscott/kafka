package shangbo.kafka.example6;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class App {
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
		Service service = context.getBean(Service.class);

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("group.id", "ConsumerGroup3"); // 消费者组的标识
		props.put("enable.auto.commit", "false"); // 收到消息后，手动提交 offset

		// 创建 Consumer, 从 topic0 接收数据
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

		// 手动设置查询 offset
		List<PartitionOffset> partitionOffsets = service.queryPartitionOffset("topic0");

		consumer.assign(partitionOffsets.stream().map(p -> new TopicPartition("topic0", p.getPartition())).collect(Collectors.toList()));

		for (PartitionOffset partitionOffset : partitionOffsets) {
			TopicPartition tp = new TopicPartition("topic0", partitionOffset.getPartition());
			consumer.seek(tp, partitionOffset.getOffset());
		}

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			service.process(records);
		}
	}
}
