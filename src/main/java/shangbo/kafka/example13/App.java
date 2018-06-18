package shangbo.kafka.example13;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.kafka.core.KafkaTemplate;

public class App {
	@SuppressWarnings({ "resource", "unchecked" })
	public static void main(String[] args) throws InterruptedException {
		ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
		
		// 使用 kafkaTemplate 发送消息
		KafkaTemplate<String, User> kafkaTemplate = context.getBean(KafkaTemplate.class);

		// 异步发送
		kafkaTemplate.send("topic4", new User("1", "ShangBo"));
		kafkaTemplate.flush();
		System.out.println("message sent");
	}
}
