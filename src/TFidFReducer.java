
/**
 * The reducer class for step1 matrix multipilcation
 * @author Elham Buxton, University of Illinois at Springfield
 */
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TFidFReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {

  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException 
  {
	  int sum = 0;	  
	  for (IntWritable value : values)
	  {
	      sum += value.get();
	  }

	  if (sum >2)
	  {
		  context.write(key, new IntWritable(sum));  
	  }
  }
}
