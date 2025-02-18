// Observatories.txt format: obs_id, name, lat, lon, country, continent, amateur
// countries with the highest number of amateur observatories
// TOP K PATTERN

/*
    - mapper 1: (Buffer, line) -> only amateur tuples (country, 1)
    - reducer 1: (country, [1,..]) -> local (country, local_max) -- setup, cleanup
    - mapper 2: local (country, local_max) -> (null, country_count)
    - reducer 2: (null, country_count) -> global (country, actual_max)
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

        // obs_id, name, lat, lon, country, continent, amateur
        String[] fields = value.toString().split(",");
        String country = fields[4];
        String amateur = fields[6];
        if (amateur.equals("True")){
            context.write(new Text(country), new IntWritable(1));
        }
    }
}


class ReducerBigData extends Reducer<Text, // Input key type
        IntWritable, // Input value type
        Text, // Output key type
        IntWritable> { // Output value type

    private int maxCount;
    private String maxCountry;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxCount = -1;
        maxCountry = null;
    }

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
        
        String country = key.toString();
        int count = 0;

        for (IntWritable v : values){
            count += v.get();
        }

        if (count >= maxCount || (count == maxCount && country.compareTo(maxCountry) <= 0)){
            maxCount = count;
            maxCountry = country;
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(new Text(maxCountry), new IntWritable(maxCount));
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
        int localMaxCountry = key.toString();
        int localMaxCount = Integer.parseInt(value.toString());
        context.write(NullWritable.get(), new Text(localMaxCountry+"_"+localMaxCount));
    }
}

public class ReducerBigData2 extends Reducer<NullWritable, Text, Text, IntWritable> {

    // reduce method
    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {

        // GLOBAL MAX 
        int globalMaxCount = -1;
        String globalMaxCountry = null;

        for (Text v : values){
            String fields = v.toString().split("_");
            String country = fields[0];
            int localMaxCount = Integer.parseInt(fields[1]);

            if (localMaxCount > globalMaxCount || (localMaxCount == globalMaxCount && contry.compareTo(globalMaxCountry) < 0)){
                globalMaxCount = localMaxCount;
                globalMaxCountry = localMaxCountry;
            }
        }

        context.write(new Text(globalMaxCountry), new IntWritable(globalMaxCount));
    }
}
