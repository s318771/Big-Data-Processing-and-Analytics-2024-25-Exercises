package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        NullWritable, // Output key type
        Text> {// Output value type

    ArrayList<String> locaTop;
    int localMax;

    protected void setup(Context context) {
        locaTop = new ArrayList<>();
        localMax = -1;
    }

    protected void map(
            Text key, // Input key type
            Text value, // Input value type
            Context context) throws IOException, InterruptedException {

        String category = key.toString();
        int count = Integer.parseInt(value.toString());

        if (count > localMax) {
            locaTop.clear();
            locaTop.add(category);
            localMax = count;
        } else if (count == localMax) {
            locaTop.add(category);
        }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        // Emit the local top category(ies)
        for (String s : locaTop) {
            context.write(NullWritable.get(), new Text(s + "_" + localMax));
        }
    }

}
