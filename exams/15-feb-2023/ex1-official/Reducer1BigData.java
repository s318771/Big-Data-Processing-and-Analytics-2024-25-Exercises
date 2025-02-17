package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<
                Text,
                IntWritable,
                Text,
                IntWritable> {

    @Override
    protected void reduce(
        Text key,
        Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {
        int sum = 0;
        String[] fields = key.toString().split(",");
        String country = fields[1];

        for(IntWritable i : values)
            sum += i.get();
        context.write(new Text(country), new IntWritable(sum));
    }
}
