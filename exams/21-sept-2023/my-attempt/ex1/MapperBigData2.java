package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            // It receives (sid_seasonNumber, nEpisodes)
            // It emits (sid, seasonNumber_nEpisodes)
            String[] sid_seasonNumber = key.toString().split(",");
            String sid = sid_seasonNumber[0];
            String seasonNumber = sid_seasonNumber[1];
            String nEpisodes = value.toString(); // I'm keeping string values for everything, we are not interested in the actual values
            context.write(new Text(sid), new Text(seasonNumber + "_" + nEpisodes));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
