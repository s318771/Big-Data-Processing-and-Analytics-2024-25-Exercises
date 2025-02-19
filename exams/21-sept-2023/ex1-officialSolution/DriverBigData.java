package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
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
    Path outputDir1;
    int numberOfReducersJob1;
    int numberOfReducersJob2;
    int exitCode;

    numberOfReducersJob1 = Integer.parseInt(args[0]);
    numberOfReducersJob2 = Integer.parseInt(args[1]);
    inputPath = new Path(args[2]);
    outputDir1 = new Path("outputPart1");
    outputDir = new Path(args[3]);

    Configuration conf = this.getConf();

    Job job = Job.getInstance(conf);

    job.setJobName("Hadoop Exam20230921 Part1");

    FileInputFormat.addInputPath(job, inputPath);
    FileOutputFormat.setOutputPath(job, outputDir1);

    job.setJarByClass(DriverBigData.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    job.setMapperClass(Mapper1BigData.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setReducerClass(Reducer1BigData.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Any value >= 1
    job.setNumReduceTasks(numberOfReducersJob1);

    // Execute the job and wait for completion
    if (job.waitForCompletion(true) == true) {
      Configuration conf2 = this.getConf();

      Job job2 = Job.getInstance(conf2);

      job2.setJobName("Hadoop Exam20230921 Part2");

      FileInputFormat.addInputPath(job2, outputDir1);
      FileOutputFormat.setOutputPath(job2, outputDir);

      job2.setJarByClass(DriverBigData.class);
      job2.setInputFormatClass(KeyValueTextInputFormat.class);
      job2.setOutputFormatClass(TextOutputFormat.class);

      job2.setMapperClass(Mapper2BigData.class);
      job2.setMapOutputKeyClass(Text.class);
      job2.setMapOutputValueClass(Text.class);

      job2.setReducerClass(Reducer2BigData.class);

      job2.setOutputKeyClass(Text.class);
      job2.setOutputValueClass(Text.class);

      // Any value >= 1
      job2.setNumReduceTasks(numberOfReducersJob2);

      if (job2.waitForCompletion(true)) {
        exitCode = 0;
      } else
        exitCode = 1;
    } else
      exitCode = 1;

    return exitCode;
  }

  /**
   * Main of the driver
   */

  public static void main(String args[]) throws Exception {
    // Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(),
        new DriverBigData(), args);

    System.exit(res);
  }
}