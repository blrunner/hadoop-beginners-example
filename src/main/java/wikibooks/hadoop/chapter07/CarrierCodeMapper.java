package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.common.CarrierCodeParser;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {
    CarrierCodeParser parser = new CarrierCodeParser(value);

    outValue.set(parser.getCarrierName());
    context.write(new TaggedKey(parser.getCarrierCode(), 0), outValue);
  }
}
