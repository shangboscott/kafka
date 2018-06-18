package shangbo.kafka.example2;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class App {
	public static void main(String[] args) {

		// Producer 配置信息，应该配置在属性文件中
		Properties props = new Properties();
		//指定要连接的 broker，不需要列出所有的 broker，但建议至少列出2个，以防某个 broker 挂了
		props.put("bootstrap.servers", "localhost:9092");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("enable.idempotence", "true");
//		props.put("retries", 3); // 不能手动设置 retries，自动设置为 Integer.MAX_VALUE
//		props.put("acks", "1"); // 不能手动设置 acks,自动设置为 all

		// 创建 Producer
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		// 发送消息
		producer.send(new ProducerRecord<String, String>("topic0", "message 4"), new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception != null) {
					System.out.println("send message4 failed with " + exception.getMessage());
				} else {
					// offset 是消息在 partition 中的编号，可以根据 offset 检索消息
					System.out.println("message4 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());	
				}
				
			}
			
		});
		
		// producer 需要关闭，放在 finally 里
		producer.close();
	}
}
