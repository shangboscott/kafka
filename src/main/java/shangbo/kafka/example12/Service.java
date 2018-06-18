package shangbo.kafka.example12;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface Service {
	Long offset(String topic, Integer partiton);

	void process(ConsumerRecord<String, String> record);

	void setDataSource(DataSource dataSource);
}
