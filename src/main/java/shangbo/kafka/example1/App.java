package shangbo.kafka.example1;

import java.util.Properties;
import java.util.concurrent.Future;

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
		props.put("retries", 3); // 如果发生错误，重试三次
		props.put("acks", "1"); // 0：不应答，1：leader 应该，all：所有 leader 和 follower 应该

		// 创建 Producer
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		
		// send 方法是异步的，方法返回并不代表消息发送成功
		producer.send(new ProducerRecord<String, String>("topic0", "message 1"));
		

		// 如果需要确认消息是否发送成功，以及发送后做一些额外操作，有两种办法
		// 方法 1: 使用 callback
		producer.send(new ProducerRecord<String, String>("topic0", "message 2"), new Callback() {

			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if(exception != null) {
					System.out.println("send message2 failed with " + exception.getMessage());
				} else {
					// offset 是消息在 partition 中的编号，可以根据 offset 检索消息
					System.out.println("message2 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());	
				}
				
			}
			
		});
		
		
		// 方法2：使用阻塞
		Future<RecordMetadata> sendResult = producer.send(new ProducerRecord<String, String>("topic0", "message 3"));
		try {
			// 阻塞直到发送成功
			RecordMetadata metadata = sendResult.get();
			System.out.println("message3 sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
		} catch(Exception e) {
			System.out.println("send message3 failed with " + e.getMessage());
		}
		
		
		// producer 需要关闭，放在 finally 里
		producer.close();
	}
}
