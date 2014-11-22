package wikibooks.hadoop.chapter07;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ReduceSideJoin extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    // Tool 인터페이스 실행
    int res = ToolRunner.run(new Configuration(), new ReduceSideJoin(), args);
    System.out.println("MR-Job Result:" + res);
  }

  public int run(String[] args) throws Exception {
    String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();

    // 입력출 데이터 경로 확인
    if (otherArgs.length != 3) {
      System.err.println("Usage: ReduceSideJoin <metadata> <in> <out>");
      System.exit(2);
    }

    // Job 이름 설정
    Job job = new Job(getConf(), "ReduceSideJoin");

    // 출력 데이터 경로 설정
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

    // Job 클래스 설정
    job.setJarByClass(ReduceSideJoin.class);
    job.setPartitionerClass(TaggedGroupKeyPartitioner.class);
    job.setGroupingComparatorClass(TaggedGroupKeyComparator.class);
    job.setSortComparatorClass(TaggedKeyComparator.class);

    // Reducer 클래스 설정
    job.setReducerClass(ReducerWithReduceSideJoin.class);

    job.setMapOutputKeyClass(TaggedKey.class);
    job.setMapOutputValueClass(Text.class);

    // 입출력 데이터 포맷 설정
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    // 출력키 및 출력값 유형 설정
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // MultipleInputs 설정
    MultipleInputs.addInputPath(job, new Path(otherArgs[0]),
      TextInputFormat.class, CarrierCodeMapper.class);
    MultipleInputs.addInputPath(job, new Path(otherArgs[1]),
      TextInputFormat.class, MapperWithReduceSideJoin.class);

    job.waitForCompletion(true);

    return 0;
  }
}