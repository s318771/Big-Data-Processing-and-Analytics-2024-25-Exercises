package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer extends org.apache.hadoop.mapreduce.Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        int count2020 = 0;
        int count2021 = 0;

        for(Text year : values) {
            String val = year.toString();
            if(val.equals("2020"))
                count2020++;
            else 
                count2021++;
        }
        if(count2021 > count2020)
            context.write(key, NullWritable.get());
    }
}
