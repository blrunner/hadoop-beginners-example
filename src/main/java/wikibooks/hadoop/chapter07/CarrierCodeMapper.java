package wikibooks.hadoop.chapter07;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import wikibooks.hadoop.common.CarrierCodeParser;

public class CarrierCodeMapper extends Mapper<LongWritable, Text, TaggedKey, Text> {
  TaggedKey outputKey = new TaggedKey();
  Text outValue = new Text();

  public void map(LongWritable key, Text value, Context context)
    throws IOException, InterruptedException {

    CarrierCodeParser parser = new CarrierCodeParser(value);

    outputKey.setCarrierCode(parser.getCarrierCode());
    outputKey.setTag(0);
    outValue.set(parser.getCarrierName());

    context.write(outputKey, outValue);
  }
}
