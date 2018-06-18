package shangbo.kafka.example9;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

public class ServiceImpl implements Service {
	private KafkaTemplate<String, String> kafkaTemplate;

	@Override
	@Transactional
	public void send(String topic, String message) {
		kafkaTemplate.send(topic, message);
		kafkaTemplate.flush();
	}

	@Override
	public void setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate) {
		this.kafkaTemplate = kafkaTemplate;
	}

}
