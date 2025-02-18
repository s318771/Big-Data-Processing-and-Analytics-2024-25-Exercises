public class MapperBigData extends Mapper<
        LongWritable, 
        Text, 
        Text, 
        IntWritable> {

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                // Input format: hid, city, country, yearBuilt
                String[] fields = value.toString().split(",");
                String city = fields[1];
                String country = fields[2];
                context.write(new Text(city + "_" + country), new IntWritable(1));
    }
}


class ReducerBigData extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
                int nHousesPerCity = 0;
                for (IntWritable value: values){
                    nHousesPerCity += value.get();
                }
                context.write(key, new IntWritable(nHousesPerCity));
    }
}


class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    // map method
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
                String country = key.toString().split(",")[1];
                int nHousesPerCity = Integer.parseInt(value.toString());
                context.write(new Text(country), new IntWritable(nHousesPerCity));
    }
}

public class ReducerBigData2 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
                int totHouses = 0;
                int countHouses = 0;
                for (IntWritable value: values){
                    totHouses += value.get();
                    countHouses++;
                }
                double avg = (double totHouses) / countHouses;
                if avg > 10000{
                    context.write(key, new DoubleWritable(avg));
                }
    }
}
