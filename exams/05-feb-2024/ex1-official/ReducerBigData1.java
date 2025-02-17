package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exam20240205 - Reducer first job
 */
class ReducerBigData1 extends Reducer<Text, // Input key type
		IntWritable, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	private int maxCount;
	private String maxCountry;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		maxCount = -1;
		maxCountry = null;
	}


	@Override
	protected void reduce(Text key, // Input key type
			Iterable<IntWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {
		
		String country = key.toString();
		int count = 0;
		for(IntWritable v : values)
			count += v.get();

		if ((count > maxCount) || (count == maxCount && country.compareTo(maxCountry) < 0)) {
			maxCount = count;
			maxCountry = country;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		// Emit the local maximum for each instance of the mapper class
		context.write(new Text(maxCountry), new IntWritable(maxCount));
	}

}
