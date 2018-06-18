package shangbo.kafka.example6;

import java.util.List;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface Service {
	List<PartitionOffset> queryPartitionOffset(String topic);
	void process(ConsumerRecords<String, String> records);
	void setDataSource(DataSource dataSource);
}
