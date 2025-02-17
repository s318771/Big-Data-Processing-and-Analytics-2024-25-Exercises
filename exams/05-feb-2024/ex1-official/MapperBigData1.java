package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exam20240205 - Mapper first job
 */

class MapperBigData1 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		// Extract the fields of the review
		// ObservatoryID,Name,Lat,Lon,Country,Continent,Amateur
		String[] fields = value.toString().split(",");
		String country = fields[4];
		String flag = fields[6];

		
		if(flag.equals("True"))
			context.write(new Text(country), new IntWritable(1));
	}
}
