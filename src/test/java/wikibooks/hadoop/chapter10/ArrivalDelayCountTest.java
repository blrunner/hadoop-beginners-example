package wikibooks.hadoop.chapter10;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import wikibooks.hadoop.chapter05.ArrivalDelayCountMapper;
import wikibooks.hadoop.chapter05.DelayCountReducer;

import java.util.ArrayList;
import java.util.List;

public class ArrivalDelayCountTest {
  private final static String inputString1 = "2008,12,13,6,1110,1103,1413,1418,DL,1641,N908DL,123,135,104,-5,7,SAT,ATL,874,8,11,0,,0,NA,NA,NA,NA,NA";
  private final static String inputString2 = "2008,12,13,6,1251,1240,1446,1437,DL,1639,N646DL,115,117,89,9,11,IAD,ATL,533,13,13,0,,0,NA,NA,NA,NA,NA";

  private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
  private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
  private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {
    ArrivalDelayCountMapper mapper = new ArrivalDelayCountMapper();
    DelayCountReducer reducer = new DelayCountReducer();
    mapDriver = MapDriver.newMapDriver(mapper);;
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
   }

  @Test
  public void testMapper1() throws Exception {
    mapDriver.withInput(new LongWritable(), new Text(inputString1));
    mapDriver.runTest();
  }

  @Test
  public void testMapper2() throws Exception {
    mapDriver.withInput(new LongWritable(), new Text(inputString2));
    mapDriver.withOutput(new Text("2008,12"), new IntWritable(1));
    mapDriver.runTest();
  }

  @Test
  public void testReducer() {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    reduceDriver.withInput(new Text("2008,12"), values);
    reduceDriver.withOutput(new Text("2008,12"), new IntWritable(2));
    reduceDriver.runTest();
  }

  @Test
  public void testMapReduceJob() throws Exception {
    mapReduceDriver.withInput(new LongWritable(1L), new Text(inputString1));
    mapReduceDriver.withInput(new LongWritable(2L), new Text(inputString2));
    mapReduceDriver.withOutput(new Text("2008,12"), new IntWritable(1));
    mapReduceDriver.runTest();
  }
}
