package shangbo.kafka.example8;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

public class App {
	@SuppressWarnings({ "resource", "unchecked" })
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
		KafkaTemplate<String, String> kafkaTemplate = context.getBean(KafkaTemplate.class);

		// 异步发送
		kafkaTemplate.send("topic0", "message x4");
		kafkaTemplate.flush();
	}
}
