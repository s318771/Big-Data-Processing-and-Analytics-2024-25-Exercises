package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper1BigData extends Mapper<
                    LongWritable,
                    Text,
                    Text,
                    IntWritable> {
    
    protected void map(
            LongWritable key,
            Text value,
            Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            String city = fields[1];
            Double sqm = Double.parseDouble(fields[3]);

            if(sqm < 60) {
                context.write(new Text(city), new IntWritable(1));
            }
    }
}
