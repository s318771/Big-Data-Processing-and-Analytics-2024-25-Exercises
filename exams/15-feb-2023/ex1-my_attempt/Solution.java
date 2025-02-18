// Countries with an average number of houses per city greater that 100 000 houses
// AVERAGE NUMBER OF HOUSES PER CITY

/*
    Idea alla base:
    - Job 1 :
        - mapper1 : (Buffer, file_line) -> (country_city, 1)
        - reducer1 : (country_city, [1,....]) -> (country, n_houses_per_city)
        - mapper2 : (country, n_houses_per_city) -> (country, n_houses_per_city) ponte per il reducer2
        - reducer2: (country, [n_houses_per_city]) -> (country, avg_per_city) filtered 

*/

public class MapperBigData extends Mapper<
        LongWritable, 
        Text, 
        Text, 
        IntWritable> {

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        
        // input format: hid, city, country, year_built
        String[] fields = value.toString().split(",");
        String country = fields[2];
        String city = fields[1];
        
        context.write(new Text(country+"_"+city), new IntWritable(1));
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
        
        String country = key.toString().split("_")[0];
        int n_houses_per_city = 0;
        for (IntWritable value: values){
            n_houses_per_city += value.get();
        }
        context.write(new Text(country), new IntWritable(n_houses_per_city));
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
        
        int n_houses_per_city = Integer.parseInt(value.toString());
        context.write(new Text(key), new IntWritable(n_houses_per_city));
    }
}

public class ReducerBigData2 extends Reducer<Text, IntWritable, Text, DoubleWritable> {

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;

        for (IntWritable value: values){
            sum += value.get();
            count++;
        }

        double mean = ((double) sum) / count;
        if (mean >= 10*1000){
            context.write(key, new DoubleWritable(mean))
        }
    }
}
