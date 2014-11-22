package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.common.AirlinePerformanceParser;

public class MapperWithReduceSideJoin extends
  Mapper<LongWritable, Text, TaggedKey, Text> {

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    AirlinePerformanceParser parser = new AirlinePerformanceParser(value);
    context.write(new TaggedKey(parser.getUniqueCarrier(), 1), value);
  }
}
