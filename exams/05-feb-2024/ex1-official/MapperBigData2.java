package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exam20240205 - Mapper second job
 */
class MapperBigData2 extends Mapper<
                    Text,  // Input key type
                    Text, 		  // Input value type
                    NullWritable,         // Output key type
                    Text> {		  // Output value type
	
    protected void map(
    		Text key,	// Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {
            
            String country = key.toString();
            String count = value.toString();

            context.write(NullWritable.get(), new Text(country + "_" + count));
            
    }
}
