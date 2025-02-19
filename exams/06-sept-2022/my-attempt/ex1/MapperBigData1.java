package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import it.polito.bigdata.Counter2021;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Counter2021> {// Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            // input: codeDC, date, kWh
            String[] fields = value.toString().split(",");
            String codeDC = fields[0];
            String year = fields[1].split("/")[0];
            Counter2021 counter2021 = new Counter2021();
            double kWh = Double.parseDouble(fields[2]);
            if(kWh >= 1000){ //only high-consumption lines
                if(year.equals("2020")){
                    counter2021.setCounter20(1);
                    context.write(new Text(codeDC), counter2021);
                } else if(year.equals("2021")){
                    counter2021.setCounter21(1);
                    context.write(new Text(codeDC), counter2021);
                }
            }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
