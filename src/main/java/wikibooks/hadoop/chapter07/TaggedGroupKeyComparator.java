package wikibooks.hadoop.chapter07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedGroupKeyComparator extends WritableComparator {

  protected TaggedGroupKeyComparator() {
    super(TaggedKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    TaggedKey k1 = (TaggedKey) w1;
    TaggedKey k2 = (TaggedKey) w2;

    // 그룹키인 항공사 코드값 정렬
    return k1.getCarrierCode().compareTo(k2.getCarrierCode());
  }
}
