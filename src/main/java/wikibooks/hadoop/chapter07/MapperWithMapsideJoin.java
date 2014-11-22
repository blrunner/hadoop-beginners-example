package wikibooks.hadoop.chapter07;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

public class MapperWithMapsideJoin extends
  Mapper<LongWritable, Text, Text, Text> {

  private Hashtable<String, String> joinMap = new Hashtable<String, String>();

  // map 출력키
  private Text outputKey = new Text();

  @Override
  public void setup(Context context) throws IOException, InterruptedException {
    try {
      // 분산캐시 조회
      Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
      // 조인 데이터 생성
      if (cacheFiles != null && cacheFiles.length > 0) {
        String line;
        String[] tokens;
        BufferedReader br = new BufferedReader(new FileReader(cacheFiles[0].toString()));
        try {
          while ((line = br.readLine()) != null) {
            tokens = line.toString().split(",");
            joinMap.put(tokens[0], tokens[1]);
          }
        } finally {
          br.close();
        }
      } else {
        System.out.println("### cache files is null!");
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    // 콤마 구분자 분리
    String[] colums = value.toString().split(",");
    if (colums != null && colums.length > 0) {
      try {
        outputKey.set(joinMap.get(colums[8]));
        context.write(outputKey, value);
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
}
