package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class Mapper2BigData extends Mapper<Text, Text, Text, Text> {

    protected void map(
            Text key,
            Text value,
            Context context) throws IOException, InterruptedException {

        int numEpisodes = Integer.parseInt(value.toString());

        // Return the key-value pair
        // key = sid
        // value = "S" if numEpisodes<=10, "L" otherwise
        if (numEpisodes <= 10)
            context.write(key, new Text("S"));
        else
            context.write(key, new Text("L"));
    }
}
