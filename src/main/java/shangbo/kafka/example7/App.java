package shangbo.kafka.example7;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class App {
	@SuppressWarnings({ "resource", "unchecked" })
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
		
		// 使用 kafkaTemplate 发送消息
		KafkaTemplate<String, String> kafkaTemplate = context.getBean(KafkaTemplate.class);

		// 异步发送
		kafkaTemplate.send("topic0", "message x1");

		// 注册 callback
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send("topic0", "message x2");
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			@Override
			public void onSuccess(SendResult<String, String> result) {
				RecordMetadata metadata = result.getRecordMetadata();
				System.out.println("message sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
			}

			@Override
			public void onFailure(Throwable ex) {
				System.out.println("send message failed with " + ex.getMessage());
			}

		});

		// 使用阻塞
		ListenableFuture<SendResult<String, String>> future2 = kafkaTemplate.send("topic0", "message x3");
		try {
			SendResult<String, String> result = future2.get();
			RecordMetadata metadata = result.getRecordMetadata();
			System.out.println("message sent to " + metadata.topic() + ", partition " + metadata.partition() + ", offset " + metadata.offset());
		}catch (Exception e) {
			System.out.println("send message failed with " + e.getMessage());
		}

	}
}
