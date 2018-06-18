package shangbo.kafka.example12;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

public class TestMessageListener implements AcknowledgingMessageListener<String, String> {
	private Service service;

	@Override
	public void onMessage(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
		System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
		service.process(record);
	}

	//
	// Setter
	//
	public void setService(Service service) {
		this.service = service;
	}

}
