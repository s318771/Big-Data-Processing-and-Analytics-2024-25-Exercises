package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *  Exam20240205 - Reducer second job
 */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,    // Output key type
                IntWritable> {  // Output value type

    // compute global maximum
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

    	int maxCount = -1;
        String maxCountry = null;

        for(Text v : values) {
            String data = v.toString();
            String[] fields = data.split("_");
            String country = fields[0];
            Integer count = Integer.parseInt(fields[1]);

            if(count > maxCount || (maxCount == count && country.compareTo(maxCountry) < 0)) {
                maxCount = count;
                maxCountry = country;
            }
        }

        // Emit the final maximum: Country and Number of amateur observatories in the selected country
        context.write(new Text(maxCountry), new IntWritable(maxCount));
        
    }
}
