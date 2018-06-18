package shangbo.kafka.example6;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

public class ServiceImpl implements Service {
	private JdbcTemplate jdbcTemplate;

	@Transactional(readOnly = true)
	public List<PartitionOffset> queryPartitionOffset(String topic) {
		String sql = "select * from kafka_offset where topic = ?";
		return jdbcTemplate.query(sql, new Object[] { topic }, new BeanPropertyRowMapper<PartitionOffset>(PartitionOffset.class));
	}

	@Transactional
	public void process(ConsumerRecords<String, String> records) {
		Set<TopicPartition> topicPartitions = records.partitions();
		for (TopicPartition tp : topicPartitions) {
			final List<ConsumerRecord<String, String>> recs = records.records(tp);

			// 保存消息
			String insertKafkaMsgSql = "insert into KAFKA_MESSAGE values (?, ?, ?, ?, ?)";
			jdbcTemplate.batchUpdate(insertKafkaMsgSql, new BatchPreparedStatementSetter() {
				public void setValues(PreparedStatement ps, int i) throws SQLException {
					ps.setString(1, recs.get(i).topic());
					ps.setInt(2, recs.get(i).partition());
					ps.setLong(3, recs.get(i).offset());
					ps.setString(4, recs.get(i).key());
					ps.setString(5, recs.get(i).value());
				}

				public int getBatchSize() {
					return recs.size();
				}
			});

			// 保存 offset
			Long maxOffset = recs.stream().mapToLong(ConsumerRecord::offset).max().getAsLong();
			maxOffset += 1;
			String updateOffsetSql = "update kafka_offset set offset = ? where topic = ? and partition = ?";
			jdbcTemplate.update(updateOffsetSql, maxOffset, tp.topic(), tp.partition());
		}
	}

	//
	// Setter
	//
	public void setDataSource(DataSource dataSource) {
		jdbcTemplate = new JdbcTemplate(dataSource);
	}

}
