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
        String mid = fields[0];
        String accepted = fields[2];

        if(accepted.equals("Yes")) {
            context.write(new Text(mid), new IntWritable(1));
        }
    }
}
