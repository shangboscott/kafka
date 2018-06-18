package shangbo.kafka.example10;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

public class TestMessageListener implements MessageListener<String, String> {

	@Override
	public void onMessage(ConsumerRecord<String, String> record) {
		System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	}

}
