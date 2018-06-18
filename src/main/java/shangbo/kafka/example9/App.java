package shangbo.kafka.example9;

import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class App {
	@SuppressWarnings({ "resource" })
	public static void main(String[] args) {
		ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
		Service service = context.getBean(Service.class);

		// 发送
		service.send("topic0", "message x5");
	}
}
