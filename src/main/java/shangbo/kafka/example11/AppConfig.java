package shangbo.kafka.example11;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;

@Configuration
public class AppConfig {

	@Bean
	public KafkaMessageListenerContainer<String, String> kafkaMessageListenerContainer(ConsumerFactory<String, String> consumerFactory, ContainerProperties containerProperties) {
		return new KafkaMessageListenerContainer<String, String>(consumerFactory, containerProperties);
	}

	@Bean
	public ConsumerFactory<String, String> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		return props;
	}
	
	@Bean
	public ContainerProperties containerProperties(MessageListener<String, String> messageListener) {
		ContainerProperties containerProperties = new ContainerProperties("topic0");
		containerProperties.setGroupId("testConsumerGroup2");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setSyncCommits(true);
		containerProperties.setAckMode(AckMode.MANUAL); // 设置手动提交
		
		return containerProperties;
	}
	
	@Bean
	public TestMessageListener messageListener() {
		return new TestMessageListener();
	}

}
