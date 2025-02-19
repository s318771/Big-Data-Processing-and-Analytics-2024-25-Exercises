package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<NullWritable, Text, Text, IntWritable> {

    @Override
    protected void reduce(
            NullWritable key,
            Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

        int max;
        String midMax;

        max = -1;
        midMax = "";

        for (Text value : values) {
            String[] fields = value.toString().split(":");
            String mid = fields[0];
            int expectedParticipants = Integer.parseInt(fields[1]);

            if (expectedParticipants > max || (expectedParticipants == max && mid.compareTo(midMax) > 0)) {
                max = expectedParticipants;
                midMax = mid;
            }

        }

        context.write(new Text(midMax), new IntWritable(max));
    }
}
