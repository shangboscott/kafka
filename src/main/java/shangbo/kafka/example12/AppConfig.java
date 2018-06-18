package shangbo.kafka.example12;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListener;
import org.springframework.kafka.listener.config.ContainerProperties;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement // 开启事务管理
public class AppConfig {
	@Bean(destroyMethod = "close")
	public BasicDataSource dataSource() {
		BasicDataSource dataSource = new BasicDataSource();
		dataSource.setDriverClassName("oracle.jdbc.driver.OracleDriver");
		dataSource.setUrl("jdbc:oracle:thin:@localhost:1521:xe");
		dataSource.setUsername("hr");
		dataSource.setPassword("123456");

		return dataSource;
	}

	@Bean
	public DataSourceTransactionManager txManager(DataSource dataSource) {
		DataSourceTransactionManager txManager = new DataSourceTransactionManager();
		txManager.setDataSource(dataSource);

		return txManager;
	}

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
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		return props;
	}

	@Bean
	public ContainerProperties containerProperties(MessageListener<String, String> messageListener, ConsumerAwareRebalanceListener consumerRebalanceListener) {
		ContainerProperties containerProperties = new ContainerProperties("topic0");
		containerProperties.setGroupId("testConsumerGroup2");
		containerProperties.setMessageListener(messageListener);
		containerProperties.setSyncCommits(true);
		containerProperties.setAckMode(AckMode.MANUAL); // 设置手动提交
		containerProperties.setConsumerRebalanceListener(consumerRebalanceListener);

		return containerProperties;
	}

	@Bean
	public TestMessageListener messageListener(Service service) {
		TestMessageListener messageListener = new TestMessageListener();
		messageListener.setService(service);
		
		return messageListener;
	}

	@Bean
	public ConsumerAwareRebalanceListener consumerAwareRebalanceListener(final Service service) {
		return new ConsumerAwareRebalanceListener() {

			@Override
			public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
				for (TopicPartition tp : partitions) {
					// offset 从 db 中查询
					long offset = service.offset("topic0", tp.partition());
					consumer.seek(tp, offset);
				}
			}

		};
	}

	@Bean
	public Service service(DataSource dataSource) {
		Service service = new ServiceImpl();
		service.setDataSource(dataSource);

		return service;
	}

}
