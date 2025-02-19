package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/*
    SOLUZIONE SBAGLIATA (CREDO)
    Mi sono confusa con la soluzione del 12/09/24
    Lì c'era da calcolare lE categoriE con il numero massimo di prodotti
    Non ho ben capito perchè questa soluzione non vada bene sinc...
    Chatty dice che ci potrebbe essere qualche problema nel partitioning da parte di Hadoop
    Però buh
    Nel dubbio fare questa cosa con setup e clearup se c'è un array di mezzo
*/



/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    Text, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

    int localMax;
    String cityWithLocalMax;
    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        localMax = -1;
        cityWithLocalMax = null;
        return;
    }
    
    protected void map(
            Text key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 
            // I receive (city, nSmallHouses)
            String city = key.toString();
            double nSmallHouses = Double.parseDouble(value.toString());
            if (nSmallHouses > localMax || (nSmallHouses == localMax && (city.compareTo(cityWithLocalMax) < 0))){
                localMax = nSmallHouses;
                cityWithLocalMax = city;
            }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        context.write(NullWritable.get(), new Text(cityWithLocalMax + "_" + localMax));
        return;
    }
}
