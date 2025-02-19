package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper2BigData extends Mapper<
        Text,
        Text,
        NullWritable,
        CityCount> {

    protected void map(
            Text key,
            Text value,
            Context context) throws IOException, InterruptedException {

        CityCount localCityCount = new CityCount();
        localCityCount.city=key.toString();
        localCityCount.count=Integer.parseInt(value.toString());


        context.write(NullWritable.get(), localCityCount);
    }
}
