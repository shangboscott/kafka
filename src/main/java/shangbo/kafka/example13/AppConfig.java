package shangbo.kafka.example13;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

@Configuration
public class AppConfig {
	//
	// 发送消息
	//
	@Bean
	public KafkaTemplate<String, User> kafkaTemplate() {
		return new KafkaTemplate<String, User>(producerFactory());
	}

	@Bean
	public ProducerFactory<String, User> producerFactory() {
		return new DefaultKafkaProducerFactory<>(producerConfigs());
	}

	@Bean
	public Map<String, Object> producerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // JsonSerializer
		props.put(ProducerConfig.RETRIES_CONFIG, 3);
		props.put(ProducerConfig.ACKS_CONFIG, "1");
		props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, "false"); // 设置不包含 header

		return props;
	}

	//
	// 接收消息
	//
	@Bean
	public KafkaMessageListenerContainer<String, User> kafkaMessageListenerContainer(ConsumerFactory<String, User> consumerFactory, ContainerProperties containerProperties) {
		return new KafkaMessageListenerContainer<String, User>(consumerFactory, containerProperties);
	}

	@Bean
	public ConsumerFactory<String, User> consumerFactory() {
		return new DefaultKafkaConsumerFactory<>(consumerConfigs());
	}

	@Bean
	public Map<String, Object> consumerConfigs() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class); // JsonDeserializer
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, User.class); // 设置 User

		return props;
	}

	@Bean
	public ContainerProperties containerProperties(MessageListener<String, User> messageListener) {
		ContainerProperties containerProperties = new ContainerProperties("topic4");
		containerProperties.setGroupId("testConsumerGroup");
		containerProperties.setMessageListener(messageListener);

		return containerProperties;
	}

	@Bean
	public TestMessageListener messageListener() {
		return new TestMessageListener();
	}

}
