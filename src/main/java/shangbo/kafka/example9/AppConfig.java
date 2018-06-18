package shangbo.kafka.example9;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement // 开启事务管理
public class AppConfig {

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate(ProducerFactory<String, String> factory) {
		return new KafkaTemplate<String, String>(factory);
	}

	@Bean
	public ProducerFactory<String, String> producerFactory() {
		DefaultKafkaProducerFactory<String, String> producerFactory = new DefaultKafkaProducerFactory<>(producerConfigs());
		producerFactory.setTransactionIdPrefix("test.transaction");
		
		return producerFactory;
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		return props;
	}

	@Bean
	public KafkaTransactionManager<String, String> kafkaTransactionManager(ProducerFactory<String, String> factory) {
		return new KafkaTransactionManager<String, String>(factory);
	}

	@Bean
	public Service service(KafkaTemplate<String, String> kafkaTemplate) {
		Service service = new ServiceImpl();
		service.setKafkaTemplate(kafkaTemplate);

		return service;
	}

}
