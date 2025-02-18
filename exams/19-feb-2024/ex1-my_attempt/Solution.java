// Purchases.txt format SaleTimeStamp, UserId, ItemId, SalePrice
// Select the users who purchased at least 50 distinct items in the period from 1/1/2020 to 31/12/2023

public class MapperBigData extends Mapper<
                LongWritable, 
                Text, 
                Text, 
                NullWritable> {

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
       // Purchases.txt format SaleTimeStamp, UserId, ItemId, SalePrice
       String[] fields = value.toString().split(",");
       String userId = fields[1];
       String itemId = fields[2];
       String timestamp = fields[0].split("-");
       // filtriamo per le date di interesse:
       if (timestamp.compareTo("2020/01/01") >= 0 and timestamp.compareTo("2023/12/31") <= 0){
        context.write(new Text(userId+"_"+itemId), NullWritable.get())
       }
    }
}

/**
 * Basic MapReduce Project - Reducer
 */
class ReducerBigData extends Reducer<Text, // Input key type
        NullWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<NullWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
        String userItem = key.toString().split("_");
        String itemId = userItem[1];
        context.write(new Text(itemId), new IntWritable(1));
    }
}

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    // map method
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        int count = Integer.parseInt(value.toString())
        context.write(key, new IntWritable(count));
    }
}

public class ReducerBigData2 extends Reducer<Text, IntWritable, ??, ??> {

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
        
        int count = 0;

        for (IntWritable v: values){
            count += v.get();
        }

        if(count >= 50){
            context.write(new Text(key), NullWritable.get())
        }
    }
}
