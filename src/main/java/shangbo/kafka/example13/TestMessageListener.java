package shangbo.kafka.example13;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class TestMessageListener implements MessageListener<String, User> {

	@Override
	public void onMessage(ConsumerRecord<String, User> record) {
		System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	}

}
