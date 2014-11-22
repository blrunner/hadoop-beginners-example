package wikibooks.hadoop.chapter06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MapFileCreator extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new MapFileCreator(), args);
    System.out.println("MR-Job Result:" + res);
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(MapFileCreator.class);
    conf.setJobName("MapFileCreator");

    // 입출력 경로 설정
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    // 입력 데이터를 SequenceFile로 설정
    conf.setInputFormat(SequenceFileInputFormat.class);
    // 출력 데이터를 MapFile로 설정
    conf.setOutputFormat(MapFileOutputFormat.class);
    // 출력 데이터의 키를 항공 운항 거리(IntWrtiable)로 설정
    conf.setOutputKeyClass(IntWritable.class);

    // 시퀀스 파일 압축 포맷 설정
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);

    JobClient.runJob(conf);

    return 0;
  }

}
