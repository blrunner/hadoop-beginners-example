package wikibooks.hadoop.chapter07;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class TaggedGroupKeyPartitioner extends Partitioner<TaggedKey, Text> {

  @Override
  public int getPartition(TaggedKey key, Text val, int numPartitions) {
    // 항공사 코드의 해시값으로 파티션 계산
    int hash = key.getCarrierCode().hashCode();
    int partition = hash % numPartitions;
    return partition;
  }
}