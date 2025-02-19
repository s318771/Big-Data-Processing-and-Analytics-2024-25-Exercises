package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
		/* Implement the reduce method */
        int globalMax = -1;
    	String cityWithGlobalMax = null;
        for (Text value: values){
            String[] fields = value.toString().split("_");
            String city = fields[0];
            int localMax = Integer.parseInt(fields[1]);
            if(localMax > globalMax || (localMax == globalMax && city.compareTo(cityWithGlobalMax) < 0)){
                globalMax = localMax;
                cityWithGlobalMax = city;
            }
        }
        context.write(new Text(cityWithGlobalMax), NullWritable.get());
    }
}