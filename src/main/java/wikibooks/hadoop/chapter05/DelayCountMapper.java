package wikibooks.hadoop.chapter05;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.common.AirlinePerformanceParser;

import java.io.IOException;

public class DelayCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
  // 작업 구분
  private String workType;
  // map 출력값
  private final static IntWritable outputValue = new IntWritable(1);
  // map 출력키
  private Text outputKey = new Text();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    workType = context.getConfiguration().get("workType");
  }

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    AirlinePerformanceParser parser = new AirlinePerformanceParser(value);

    // 출발 지연 데이터 출력
    if (workType.equals("departure")) {
      if (parser.getDepartureDelayTime() > 0) {
        // 출력키 설정
        outputKey.set(parser.getYear() + "," + parser.getMonth());
        // 출력 데이터 생성
        context.write(outputKey, outputValue);
      }
    // 도착 지연 데이터 출력
    } else if (workType.equals("arrival")) {
      if (parser.getArriveDelayTime() > 0) {
        // 출력키 설정
        outputKey.set(parser.getYear() + "," + parser.getMonth());
        // 출력 데이터 생성
        context.write(outputKey, outputValue);
      }
    }
  }
}
