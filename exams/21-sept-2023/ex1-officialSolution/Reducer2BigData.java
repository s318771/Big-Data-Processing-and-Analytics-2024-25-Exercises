package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key,
            Iterable<Text> values,
            Context context) throws IOException, InterruptedException {

        int longSeason = 0;
        int shortSeason = 0;

        for (Text value : values) {
            if (value.toString().compareTo("L") == 0) {
                longSeason++;
            } else {
                shortSeason++;
            }
        }

        context.write(key, new Text(longSeason + "," + shortSeason));
    }
}
