
/**
 * The driver class for step1 matrix multipilcation running a chain of two mapreduce jobs
 * @author Elham Buxton, University of Illinois at Springfield
 */
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
//import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
//import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFidF extends Configured implements Tool{
 
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new TFidF(),args);
	    System.exit(exitCode);
  }

	@Override
	public int run(String[] args) throws Exception {
		 if (args.length != 2) {
		      //System.err.println("Usage: MatrixMult <input path> <output path>");
		      //System.exit(-1);
		    }
		    Configuration conf = new Configuration();
		    conf.set("xmlinput.start", "<page>");
		    conf.set("xmlinput.end", "</page>");
		    conf.set("io.serializations", "org.apache.hadoop.io.serializer.JavaSerialization,org.apache.hadoop.io.serializer.WritableSerialization");
		    Job job1 = new Job(conf);
		    job1.setJarByClass(TFidF.class);
		    job1.setJobName("TFIDFStep1");
		    //Create a temporary file to store the result of job1
		    FileInputFormat.addInputPath(job1, new Path(args[args.length-2]));
		    Path tempOut = new Path("temp");
		    SequenceFileOutputFormat.setOutputPath(job1, tempOut);
		    job1.setInputFormatClass(XmlInputFormat.class);
		    job1.setOutputFormatClass(SequenceFileOutputFormat.class);

		    job1.setMapperClass(TFidFMapper.class);
		    //job1.setCombinerClass(TFidFReducer.class);
		    job1.setReducerClass(TFidFReducer.class);
		    job1.setNumReduceTasks(20);
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(IntWritable.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(IntWritable.class);
		    job1.waitForCompletion(true);

		    //Job2 is the mapreduce job for the second step of matrix multiplication
		    Configuration conf2 = new Configuration();
		    long milliSeconds = 1000*60*60; 
	        conf2.setLong("mapred.task.timeout", milliSeconds);
		    Job job2 = new Job(conf2);
		    job2.setJarByClass(TFidF.class);
		    job2.setJobName("TFIDFStep2");
		    
		    //The input of job2 is the output of job 1
		    job2.setInputFormatClass(SequenceFileInputFormat.class);
		    SequenceFileInputFormat.addInputPath(job2, tempOut);
		    FileOutputFormat.setOutputPath(job2, new Path(args[args.length-1]));
		    job2.setMapperClass(TFidF2Mapper.class);
		    job2.setReducerClass(TFidF2Reducer.class);
		    job2.setNumReduceTasks(20);
		    job2.setMapOutputKeyClass(Text.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(DoubleWritable.class);
		    job2.waitForCompletion(true);
		    return(job2.waitForCompletion(true) ? 0 : 1);
	}
}
