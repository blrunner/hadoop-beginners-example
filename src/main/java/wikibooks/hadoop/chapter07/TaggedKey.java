package wikibooks.hadoop.chapter07;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TaggedKey implements WritableComparable<TaggedKey> {
  // 항공사 코드
  private String carrierCode;
  // 조인 테그
  private Integer tag;

  public TaggedKey() {}

  public TaggedKey(String carrierCode, int tag) {
    this.carrierCode = carrierCode;
    this.tag = tag;
  }

  public String getCarrierCode() {
    return carrierCode;
  }

  public Integer getTag() {
    return tag;
  }

  public void setCarrierCode(String carrierCode) {
    this.carrierCode = carrierCode;
  }

  public void setTag(Integer tag) {
    this.tag = tag;
  }

  @Override
  public int compareTo(TaggedKey key) {
    int result = this.carrierCode.compareTo(key.carrierCode);

    if (result == 0) {
      return  this.tag.compareTo(key.tag);
    }

    return result;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeString(out, carrierCode);
    out.writeInt(tag);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    carrierCode = WritableUtils.readString(in);
    tag = in.readInt();
  }
}
