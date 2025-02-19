package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer1BigData extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context) throws IOException, InterruptedException {

        int numEpisodes = 0;

        for (IntWritable val : values) {
            numEpisodes += val.get();
        }

        String sid = key.toString().split(",")[0];

        // Return the key-value pair
        // key = sid
        // value = numEpisodes
        // The identifier of the season is not needed anymore
        context.write(new Text(sid), new IntWritable(numEpisodes));
    }

}
