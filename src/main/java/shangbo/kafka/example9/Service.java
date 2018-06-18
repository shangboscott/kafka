package shangbo.kafka.example9;

import org.springframework.kafka.core.KafkaTemplate;

public interface Service {
	void send(String topic, String message);

	void setKafkaTemplate(KafkaTemplate<String, String> kafkaTemplate);
}
