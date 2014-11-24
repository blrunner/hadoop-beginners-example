package wikibooks.hadoop.chapter07;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerWithReduceSideJoin extends Reducer<TaggedKey, Text, Text, Text> {
  // 출력키
  private Text outputKey = new Text();
  // 출력값
  private Text outputValue = new Text();

  public void reduce(TaggedKey key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {

    Iterator<Text> iterator = values.iterator();
    // 항공사 이름 조회
    Text carrierName = new Text(iterator.next());
    // 운항 지연 레코드 조회
    while (iterator.hasNext()) {
      Text record = iterator.next();
      outputKey.set(key.getCarrierCode());
      outputValue = new Text(carrierName.toString() + "\t" + record.toString());
      context.write(outputKey, outputValue);
    }
  }
}