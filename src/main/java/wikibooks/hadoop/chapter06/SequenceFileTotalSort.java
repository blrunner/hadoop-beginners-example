package wikibooks.hadoop.chapter06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class SequenceFileTotalSort extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new Configuration(), new SequenceFileTotalSort(), args);
    System.out.println("MR-Job Result:" + res);
  }

  public int run(String[] args) throws Exception {
    JobConf conf = new JobConf(getConf(), SequenceFileTotalSort.class);
    conf.setJobName("SequenceFileTotalSort");

    conf.setInputFormat(SequenceFileInputFormat.class);
    conf.setOutputFormat(SequenceFileOutputFormat.class);
    conf.setOutputKeyClass(IntWritable.class);
    conf.setPartitionerClass(TotalOrderPartitioner.class);

    // SequenceFile 압축 포맷 설정
    SequenceFileOutputFormat.setCompressOutput(conf, true);
    SequenceFileOutputFormat.setOutputCompressorClass(conf, GzipCodec.class);
    SequenceFileOutputFormat.setOutputCompressionType(conf, CompressionType.BLOCK);

    // 입출력 경로 설정
    FileInputFormat.setInputPaths(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));

    Path inputDir = FileInputFormat.getInputPaths(conf)[0];
    inputDir = inputDir.makeQualified(inputDir.getFileSystem(conf));
    Path partitionFile = new Path(inputDir, "_partitions");
    TotalOrderPartitioner.setPartitionFile(conf, partitionFile);

    // 샘플 데이터 추출
    InputSampler.Sampler<IntWritable, Text> sampler = new InputSampler.RandomSampler<IntWritable, Text>(
      0.1, 1000, 10);
    InputSampler.writePartitionFile(conf, sampler);

    URI partitionUri = new URI(partitionFile.toString() + "#_partitions");
    DistributedCache.addCacheFile(partitionUri, conf);
    DistributedCache.createSymlink(conf);

    JobClient.runJob(conf);

    return 0;
  }
}
