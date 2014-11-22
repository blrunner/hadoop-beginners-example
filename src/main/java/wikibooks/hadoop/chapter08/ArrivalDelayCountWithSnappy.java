package wikibooks.hadoop.chapter08;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import wikibooks.hadoop.chapter05.ArrivalDelayCountMapper;
import wikibooks.hadoop.chapter05.DelayCountReducer;

public class ArrivalDelayCountWithSnappy {
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    //Map 출력 압축 설정
    conf.setBoolean("mapred.compress.map.output", true);
    conf.set("mapred.map.output.compression.codec",
      "org.apache.hadoop.io.compress.SnappyCodec");

    //입출력 데이터 경로 확인
    if (args.length != 2) {
      System.err
        .println("Usage: ArrivalDelayCountWithSnappy <input> <output>");
      System.exit(2);
    }
    //Job 이름 설정
    Job job = new Job(conf, "ArrivalDelayCountWithSnappy");

    //입출력 데이터 경로 설정
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    //Job 클래스 설정
    job.setJarByClass(ArrivalDelayCountWithSnappy.class);
    //Mapper 클래스 설정
    job.setMapperClass(ArrivalDelayCountMapper.class);
    //Reducer 클래스 설정
    job.setReducerClass(DelayCountReducer.class);

    //입출력 데이터 포맷 설정
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    //시퀀스 파일 설정
    SequenceFileOutputFormat.setCompressOutput(job, true);
    SequenceFileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.BLOCK);

    //출력키 및 출력값 유형 설정
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.waitForCompletion(true);
  }
}
