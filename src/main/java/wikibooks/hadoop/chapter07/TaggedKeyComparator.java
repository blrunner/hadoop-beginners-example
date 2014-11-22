package wikibooks.hadoop.chapter07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TaggedKeyComparator extends WritableComparator {
  protected TaggedKeyComparator() {
    super(TaggedKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    TaggedKey k1 = (TaggedKey) w1;
    TaggedKey k2 = (TaggedKey) w2;

    int cmp = k1.getCarrierCode().compareTo(k2.getCarrierCode());
    if (cmp != 0) {
      return cmp;
    }

    return k1.getTag().compareTo(k2.getTag());
  }
}
