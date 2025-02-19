package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Counter2021,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Counter2021> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        // Receives key = codDC and value = counter2021
        int count20 = 0;
        int count21 = 0;
    	for(Counter2021 value: values){
            count20 += value.getCounter20();
            count21 += value.getCounter21();
        }
        if (count21 > count20){
            context.write(key, NullWritable.get());
        }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
