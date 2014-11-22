package wikibooks.hadoop.chapter10;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import wikibooks.hadoop.chapter04.WordCountMapper;
import wikibooks.hadoop.chapter04.WordCountReducer;

public class WordCount extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new WordCount(), args);
    System.out.println("## RESULT:" + res);
  }

  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입출력 데이터 경로 확인
    if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }
    // Job 이름 설정
    Job job = new Job(getConf(), "WordCount");

    job.setJarByClass(WordCount.class);
    job.setMapperClass(WordCountMapper.class);
    job.setReducerClass(WordCountReducer.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
    return 0;
  }
}