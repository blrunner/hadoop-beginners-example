package wikibooks.hadoop.chapter10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import wikibooks.hadoop.chapter04.WordCountMapper;
import wikibooks.hadoop.chapter04.WordCountReducer;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class WordCountTest {

  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  private final static String inputString = "apache hadoop apache hbase apache tajo";

  @Before
  public void setUp() {
    WordCountMapper mapper = new WordCountMapper();
    WordCountReducer reducer = new WordCountReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws Exception {
    mapDriver.withInput(new LongWritable(1L), new Text(inputString));
    mapDriver.withOutput(new Text("apache"), new IntWritable(1));
    mapDriver.withOutput(new Text("hadoop"), new IntWritable(1));
    mapDriver.withOutput(new Text("apache"), new IntWritable(1));
    mapDriver.withOutput(new Text("hbase"), new IntWritable(1));
    mapDriver.withOutput(new Text("apache"), new IntWritable(1));
    mapDriver.withOutput(new Text("tajo"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() throws Exception {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("a"), values);
    reduceDriver.withOutput(new Text("a"), new IntWritable(2));
    reduceDriver.runTest();
  }

  @Test
  public void testMapReduceJob() throws Exception {
    mapReduceDriver.withInput(new LongWritable(1L), new Text(inputString));
    mapReduceDriver.withOutput(new Text("apache"), new IntWritable(3));
    mapReduceDriver.withOutput(new Text("hadoop"), new IntWritable(1));
    mapReduceDriver.withOutput(new Text("hbase"), new IntWritable(1));
    mapReduceDriver.withOutput(new Text("tajo"), new IntWritable(1));
    mapReduceDriver.runTest();
  }
}
