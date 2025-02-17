package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<
        Text,
        IntWritable,
        Text,
        DoubleWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for(IntWritable i : values) {
            sum += i.get();
            count++;
        }

        double mean = ((double) sum) / count;
        if(mean > 10000)
            context.write(key, new DoubleWritable(mean));
    }
}
