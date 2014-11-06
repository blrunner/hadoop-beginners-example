package wikibooks.hadoop.chapter05;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DepartureDelayCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

  // map 출력값
	private final static IntWritable outputValue = new IntWritable(1);
  // map 출력키
	private Text outputKey = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		if (key.get() > 0) {
      // 콤마 구분자 분리
			String[] colums = value.toString().split(",");
			if (colums != null && colums.length > 0) {
				try {
          // 출력키 설정
					outputKey.set(colums[0] + "," + colums[1]);
					if (!colums[15].equals("NA")) {
						int depDelayTime = Integer.parseInt(colums[15]);
						if (depDelayTime > 0) {
              // 출력 데이터 생성
							context.write(outputKey, outputValue);
						}
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

	}
}
