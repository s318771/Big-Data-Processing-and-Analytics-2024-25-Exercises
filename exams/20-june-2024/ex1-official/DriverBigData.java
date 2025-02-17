package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
MapReduce program exam 2024-06-20

Process JobOffers.txt to count
-  rejected offers with a salary higher than 100,000 euros --> these must be more than 10
-  accepted offers --> these must be zero
Emits (jobID, [rejectedCount,acceptedCount]) to count in the reducer.
The reducer counts the number of rejected and accepted offers per job ID.
Emits JobID and the number of rejected offers if
- there are more than 10 rejected offers with a salary higher than 100,000 euros and 
- there are no accepted offers.
 */

public class DriverBigData extends Configured 
implements Tool {

  @Override
  public int run(String[] args) throws Exception {

	Path inputPathJob1; // inputPathJobOffers, inputPathJobPostings;
	Path outputDirJob1; // outputDirJob2;
	int numberOfReducers;
	int exitCode;

	// Parse the parameters
	numberOfReducers = 3; // Integer.parseInt(args[0]);
	inputPathJob1 = new Path("./input_data_MR/JobOffers.txt");
	outputDirJob1 = new Path("./output");

	Configuration conf = this.getConf();

	// First job

	// Define a new job
	Job job1 = Job.getInstance(conf);

	// Assign a name to the job1
	// it filters high salary rejected offers
	job1.setJobName("Exam 20240620 - MapReduce exercise - job 1");

	// Set path of the input file/folder (if it is a folder, the job reads
	// all the files in the specified folder) for this job
	FileInputFormat.addInputPath(job1, inputPathJob1);
	//FileInputFormat.addInputPath(job1, inputPathJobOffers);  	// Path to JobOffers.txt
	//FileInputFormat.addInputPath(job1, inputPathJobPostings);  	// Path to JobPostings.txt

	// Set path of the output folder for this job
	FileOutputFormat.setOutputPath(job1, outputDirJob1);

	// Specify the class of the Driver for this job
	job1.setJarByClass(DriverBigData.class);

	// Set job input format
	job1.setInputFormatClass(TextInputFormat.class);

	// Set job output format
	job1.setOutputFormatClass(TextOutputFormat.class);

	// Set map class
	job1.setMapperClass(Mapper1.class);

	// Set map output key and value classes
	job1.setMapOutputKeyClass(Text.class);
	job1.setMapOutputValueClass(OfferStatusCounter.class);

	// Set reduce class
	job1.setReducerClass(Reducer1.class);

	// Set reduce output key and value classes
	job1.setOutputKeyClass(Text.class);
	job1.setOutputValueClass(Text.class);

	// Set number of reducers
	job1.setNumReduceTasks(numberOfReducers);

	// Execute the job and wait for completion
	if (job1.waitForCompletion(true) == true)
		exitCode = 0;
	else
		exitCode = 1;

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