package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * MapReduce program
 */
public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {

      Path inputPath;
      Path outputDir;
      int numberOfReducers;
      int exitCode;

      // Parse the parameters
      // Number of instances of the reducer class
      numberOfReducers = Integer.parseInt(args[0]); // Any value >=1
      // Folder containing the input data
      inputPath = new Path(args[1]);
      // Output folder
      outputDir = new Path(args[2]);

      Configuration conf = this.getConf();

      // Define a new job
      Job job = Job.getInstance(conf);

      // Assign a name to the job
      job.setJobName("Exam20220906 - Hadoop");

      // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
      FileInputFormat.addInputPath(job, inputPath);

      // Set path of the output folder for this job
      FileOutputFormat.setOutputPath(job, outputDir);

      // Specify the class of the Driver for this job
      job.setJarByClass(DriverBigData.class);

      // Set job input format
      job.setInputFormatClass(TextInputFormat.class);

      // Set job output format
      job.setOutputFormatClass(TextOutputFormat.class);

      // Set map class
      job.setMapperClass(Mapper.class);

      // Set map output key and value classes
      job.setMapOutputKeyClass(Text.class);
      job.setMapOutputValueClass(Text.class);

      // Set reduce class
      job.setReducerClass(Reducer.class);

      // Set reduce output key and value classes
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(NullWritable.class);

      // Set number of reducers
      job.setNumReduceTasks(numberOfReducers);


      // Execute the job and wait for completion
      if (job.waitForCompletion(true)==true)
          exitCode = 0;
      else
          exitCode=1;

      return exitCode;
  }
  

  /** Main of the driver
   */
  
  public static void main(String args[]) throws Exception {
	// Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), 
    		new DriverBigData(), args);

    System.exit(res);
  }
}
