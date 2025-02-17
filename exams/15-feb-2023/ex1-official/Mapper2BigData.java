package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper2BigData extends Mapper<
        Text,
        Text,
        Text,
        IntWritable> {

    protected void map(
            Text key,
            Text value,
            Context context) throws IOException, InterruptedException {

        int v = Integer.parseInt(value.toString());
        context.write(key, new IntWritable(v));
    }
}
