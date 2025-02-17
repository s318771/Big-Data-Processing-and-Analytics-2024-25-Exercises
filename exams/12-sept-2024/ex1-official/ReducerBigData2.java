package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerBigData2 extends Reducer<NullWritable, Text, Text, NullWritable>{

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        ArrayList<String> acc = new ArrayList<>();
        int maxCount = -1;

        String category, count;
        int value_count;
        
        for(Text v : values) {
            String tmp = v.toString();
            category = tmp.split("_")[0];
            count = tmp.split("_")[1];
            value_count = Integer.parseInt(count);

            if (value_count > maxCount) {
                acc.clear();
                acc.add(category);
                maxCount = value_count;
            }
            else if (value_count == maxCount)
                acc.add(category);
        }

        for(String s : acc)
            context.write(new Text(s), NullWritable.get());
    }
    
}
