package wikibooks.hadoop.chapter06;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

public class DelayCountReducerWithDateKey extends
  Reducer<DateKey, IntWritable, DateKey, IntWritable> {

  private MultipleOutputs<DateKey, IntWritable> mos;

  // reduce 출력키
  private DateKey outputKey = new DateKey();

  // reduce 출력값
  private IntWritable result = new IntWritable();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    mos = new MultipleOutputs<DateKey, IntWritable>(context);
  }

  public void reduce(DateKey key, Iterable<IntWritable> values,
                     Context context) throws IOException, InterruptedException {
    // 콤마 구분자 분리
    String[] colums = key.getYear().split(",");

    int sum = 0;
    Integer bMonth = key.getMonth();

    if (colums[0].equals("D")) {
      for (IntWritable value : values) {
        if (bMonth != key.getMonth()) {
          result.set(sum);
          outputKey.setYear(key.getYear().substring(2));
          outputKey.setMonth(bMonth);
          mos.write("departure", outputKey, result);
          sum = 0;
        }
        sum += value.get();
        bMonth = key.getMonth();
      }
      if (key.getMonth() == bMonth) {
        outputKey.setYear(key.getYear().substring(2));
        outputKey.setMonth(key.getMonth());
        result.set(sum);
        mos.write("departure", outputKey, result);
      }
    } else {
      for (IntWritable value : values) {
        if (bMonth != key.getMonth()) {
          result.set(sum);
          outputKey.setYear(key.getYear().substring(2));
          outputKey.setMonth(bMonth);
          mos.write("arrival", outputKey, result);
          sum = 0;
        }
        sum += value.get();
        bMonth = key.getMonth();
      }
      if (key.getMonth() == bMonth) {
        outputKey.setYear(key.getYear().substring(2));
        outputKey.setMonth(key.getMonth());
        result.set(sum);
        mos.write("arrival", outputKey, result);
      }
    }
  }

  @Override
  public void cleanup(Context context) throws IOException,
    InterruptedException {
    mos.close();
  }
}
