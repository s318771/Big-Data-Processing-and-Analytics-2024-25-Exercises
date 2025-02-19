package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;


/**
 * Basic MapReduce Project - Mapper
 */
class Mapper extends org.apache.hadoop.mapreduce.Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type

    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(",");
            String year = fields[1].split("/")[0];
            String codDC = fields[0];
            double pwrCons = Double.parseDouble(fields[2]);

            if(year.equals("2020") || year.equals("2021"))
                if(pwrCons >= 1000)
                    context.write(new Text(codDC), new Text(year));
    }
}
