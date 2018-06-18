package shangbo.kafka.example6;

public class PartitionOffset {
	private Integer partition;
	private Long offset;
	
	public Integer getPartition() {
		return partition;
	}
	public void setPartition(Integer partition) {
		this.partition = partition;
	}
	public Long getOffset() {
		return offset;
	}
	public void setOffset(Long offset) {
		this.offset = offset;
	}
}
