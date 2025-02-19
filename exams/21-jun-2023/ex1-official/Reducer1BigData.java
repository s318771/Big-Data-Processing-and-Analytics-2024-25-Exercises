package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<Text, IntWritable, Text, IntWritable> {

    private int max;
    private String midMax;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        this.max = -1;
        this.midMax = "";
    }

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        String mid = key.toString();
        for (IntWritable val : values) {
            sum += val.get();
        }
        if (sum > this.max || (sum == this.max && mid.compareTo(this.midMax) > 0)) {
            this.max = sum;
            this.midMax = mid;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        if (this.max != -1) {
            context.write(new Text(this.midMax), new IntWritable(this.max));
        }
    }
}
