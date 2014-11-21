package wikibooks.hadoop.chapter06;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class DateKeyComparator extends WritableComparator {
  protected DateKeyComparator() {
    super(DateKey.class, true);
  }

  @SuppressWarnings("rawtypes")
  @Override
  public int compare(WritableComparable w1, WritableComparable w2) {
    //복합키 클래스 캐스팅
    DateKey k1 = (DateKey) w1;
    DateKey k2 = (DateKey) w2;

    //연도 비교
    int cmp = k1.getYear().compareTo(k2.getYear());
    if (cmp != 0) {
      return cmp;
    }

    //월 비교
    return k1.getMonth() == k2.getMonth() ? 0 : (k1.getMonth() < k2
      .getMonth() ? -1 : 1);
  }
}
