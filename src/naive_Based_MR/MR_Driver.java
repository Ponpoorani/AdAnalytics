package naive_Based_MR;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class MR_Driver implements Tool{
	Configuration conf = null;
	
	public int run(String[] args) throws Exception {
		// Check for valid number of arguments.
		if (args.length < 3) {
			System.err.println("*** Error: Missing Parameters *** \n " +
									   "Usage: hadoop Driver <input_path> <output_path> <log_type>");
			System.exit(-1);
		}
		long start=System.nanoTime();
		Configuration conf = getConf();
		conf.set("log_type", args[2]);
		
		/**
		 * Create a new job object and set the output types of the Map and Reduce function.
		 * Also set Mapper and Reducer classes.
		 */
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "ETL MR");
		job.setJarByClass(MR_Driver.class);
		job.setMapperClass(MR_mapper.class);
		 
		job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		 
		// the HDFS input and output directory to be fetched from the command line
		FileInputFormat.addInputPath(job,new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		 
		if(job.waitForCompletion(true)){
			System.out.println("Successfull Job in: "+(System.nanoTime()-start)/1000000000);
		}
		
	    return (job.waitForCompletion(true) ? 0 : 1); 
	}

	public Configuration getConf() {
		conf = new Configuration();
		return conf;
	}


	@Override
	public void setConf(Configuration arg0) {}
	
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new MR_Driver(), args);
		System.exit(result);
	}

}
