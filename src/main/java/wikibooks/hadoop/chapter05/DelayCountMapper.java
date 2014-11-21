package wikibooks.hadoop.chapter05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class DelayCountMapper extends
  Mapper<LongWritable, Text, Text, IntWritable> {

  // map 출력값
  private final static IntWritable outputValue = new IntWritable(1);
  // 작업 구분
  private String workType;
  // map 출력키
  private Text outputKey = new Text();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    workType = context.getConfiguration().get("workType");
  }

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    if (key.get() > 0) {
      // 콤마 구분자 분리
      String[] colums = value.toString().split(",");
      if (colums != null && colums.length > 0) {
        try {
          // 출발 지연 데이터 출력
          if (workType.equals("departure")) {
            if (!colums[15].equals("NA")) {
              int depDelayTime = Integer.parseInt(colums[15]);
              if (depDelayTime > 0) {
                // 출력키 설정
                outputKey.set(colums[0] + "," + colums[1]);
                // 출력 데이터 생성
                context.write(outputKey, outputValue);
              }
            }
            // 도착 지연 데이터 출력
          } else if (workType.equals("arrival")) {
            if (!colums[14].equals("NA")) {
              int arrDelayTime = Integer.parseInt(colums[14]);
              if (arrDelayTime > 0) {
                // 출력키 설정
                outputKey.set(colums[0] + "," + colums[1]);
                // 출력 데이터 생성
                context.write(outputKey, outputValue);
              }
            }
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
  }
}
