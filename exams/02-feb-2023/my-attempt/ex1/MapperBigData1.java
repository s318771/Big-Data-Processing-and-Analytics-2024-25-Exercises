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
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            // input format hid, city, country, sizeSQM
            String[] fields = value.toString().split(",");
            String city = fields[1];
            // double sizeSQM = Integer.parseInt(fields[3]); // ATTENZIONE QUA
            Double sizeSQM = Double.parseDouble(fields[3]);
            if (sizeSQM < 60){
                context.write(new Text(city), new IntWritable(1));
            }
    }
}
