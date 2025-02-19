package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer
 */
class Reducer2BigData extends Reducer<
        NullWritable,
        CityCount,
        Text,
        IntWritable> {



    @Override
    protected void reduce(
            NullWritable key,
            Iterable<CityCount> values,
            Context context) throws IOException, InterruptedException {
                
            String maxCity=null;
            int maxCount=-1;


            for(CityCount v : values) {
                if(maxCount == -1 || v.count > maxCount || 
                (v.count == maxCount && v.city.compareTo(maxCity) < 0)) {
                    maxCount = v.count;
                    maxCity = v.city;
                }
            }

            context.write(new Text(maxCity), new IntWritable(maxCount));
    }

}
