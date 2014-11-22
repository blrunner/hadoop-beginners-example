package wikibooks.hadoop.chapter06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SearchValueList extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SearchValueList(), args);
    System.out.println("MR-Job Result:" + res);
  }

  public int run(String[] args) throws Exception {
    Path path = new Path(args[0]);
    FileSystem fs = path.getFileSystem(getConf());

    // MapFile 조회
    Reader[] readers = MapFileOutputFormat.getReaders(fs, path, getConf());

    // 검색 키를 저장할 객체를 선언
    IntWritable key = new IntWritable();
    key.set(Integer.parseInt(args[1]));

    // 검색 값을 저장할 객체를 선언
    Text value = new Text();

    // 파티셔너를 이용해 검색 키가 저장된 MapFile 조회
    Partitioner<IntWritable, Text> partitioner = new HashPartitioner<IntWritable, Text>();
    Reader reader = readers[partitioner.getPartition(key, value, readers.length)];

    // 검색 결과 확인
    Writable entry = reader.get(key, value);
    if (entry == null) {
      System.out.println("The requested key was not found.");
    }

    // MapFile을 순회하며 키와 값을 출력
    IntWritable nextKey = new IntWritable();
    do {
      System.out.println(value.toString());
    } while (reader.next(nextKey, value) && key.equals(nextKey));

    return 0;
  }
}
