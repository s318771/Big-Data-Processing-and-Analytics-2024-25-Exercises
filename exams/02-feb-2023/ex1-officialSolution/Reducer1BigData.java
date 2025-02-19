package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<Text, IntWritable, Text, IntWritable> {

    private String maxCity;
    private int maxCount;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.maxCity = null;
        this.maxCount = -1;
    }

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int val = -1;
        int sum = 0;
        String city = key.toString();
        for (IntWritable v : values) {
            val = v.get();
            sum += val;
        }

        if (maxCount == -1 || sum > maxCount || (sum == maxCount && city.compareTo(maxCity) < 0)) {
            maxCount = sum;
            maxCity = city;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (this.maxCity != null) {
            context.write(new Text(maxCity), new IntWritable(maxCount));
        }
    }
}
