package wikibooks.hadoop.chapter03;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MyWritableComparable implements WritableComparable {
  private int counter;
  private long timestamp;

  public void write(DataOutput out) throws IOException {
    out.writeInt(counter);
    out.writeLong(timestamp);
  }

  public void readFields(DataInput in) throws IOException {
    counter = in.readInt();
    timestamp = in.readLong();
  }

  @Override
  public int compareTo(Object o) {
    MyWritableComparable w = (MyWritableComparable)o;
    if(counter > w.counter) {
      return -1;
    } else if(counter < w.counter) {
      return 1;
    } else {
      if(timestamp < w.timestamp) {
        return 1;
      } else if(timestamp > w.timestamp) {
        return -1;
      } else {
        return 0;
      }
    }
  }
}
