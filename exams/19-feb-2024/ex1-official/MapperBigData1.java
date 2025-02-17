package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper first job
 */
class MapperBigData1 extends Mapper<LongWritable, // Input key type
		Text, // Input value type
		Text, // Output key type
		NullWritable> { // Output value type

	protected void map(LongWritable key, // Input key type
			Text value, // Input value type
			Context context) throws IOException, InterruptedException {

		String[] fields = value.toString().split(",");
		String userId = fields[1];
		String productId = fields[2];
		String timestamp = fields[0].split("-")[0];

		// filter based on date of purchase
		if(timestamp.compareTo("2020/01/01") >= 0 && 
			timestamp.compareTo("2023/12/21") <= 0)
			context.write(new Text(userId + "_" + productId), NullWritable.get());
	}
}
