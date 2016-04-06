/**
 * The mapper class for step2 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 */
import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class TFidF2Mapper extends
    Mapper< Text, IntWritable, Text, Text> {
  
	 
	public void map(Text key,  IntWritable value, Context context)
      throws IOException, InterruptedException {
      int sep = key.toString().indexOf(",");
      String term = key.toString().substring(0, sep);
      String doc = key.toString().substring(sep+1);     
      context.write(new Text(term), new Text(Long.toString(value.get())+"}"+doc+"}"+Integer.toString(1)));
  }
}