package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer first job
 */
class ReducerBigData1 extends Reducer<Text, // Input key type
		NullWritable, // Input value type
		Text, // Output key type
		IntWritable> { // Output value type

	@Override
	protected void reduce(Text key, // Input key type
			Iterable<NullWritable> values, // Input value type
			Context context) throws IOException, InterruptedException {

		// the reduce method guarantees that for each user we obtain the distinct items
		// the user purchased
		String userProduct = key.toString();
		String userId = userProduct.split("_")[0];

		context.write(new Text(userId), new IntWritable(1));
	}

}
