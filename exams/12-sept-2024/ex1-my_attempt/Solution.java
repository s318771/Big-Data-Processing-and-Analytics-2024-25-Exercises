// Products.txt  prod_id, name, category
// Categories with the largest number of products

/*
    job 1: conta il numero di prodotti per ciascuna categoria
    mapper 1: (buffer, file_line) -> (category, 1)
    reducer 1: (category, [1, ..]) -> (category, n_prods)
    job 2: determina quali categorie hanno il numero max di prodotti
    mapper 2: (category, n_prods) -> (null, category_maxNProd)  -- setup, cleanup
    reducer 2: (null, category_maxNProd) -> (categoryWithActualMax, null)
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
        // prod_id, name, category
        String[] fields = value.toString().split(",");
        String category = fields[2];
        context.write(new Text(category), new IntWritable(1));
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
        
        int n_prods = 0;

        for (IntWritable v: values){
            n_prods += v.get();
        }

        context.write(key, new IntWritable(n_prods));
    }
}


class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        NullWritable, // Output key type
        Text> { // Output value type

    int Local_maxNProds;
    ArrayList<String> local_countryMaxProds;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        maxNProds = -1;
        local_countryMaxProds = new ArrayList<>();
    }

    // map method
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {
        // i receive (country, n_prods)
        String country = key.toString();
        int n_prods = Integer.parseInt(value.toString());

        if (n_prods > maxNProds){
            local_countryMaxProds.clear();
            local_countryMaxProds.add(country);
            maxNProds = n_prods;
        } else if (n_prods == maxNProds){
            local_countryMaxProds.add(country);
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (String s: local_countryMaxProds){
            context.write(NullWritable.get(), new Text(s+"_"+maxNProds));
        }
    }
}

public class ReducerBigData2 extends Reducer<NullWritable, Text, ??, ??> {

    // reduce method
    @Override
    protected void reduce(
            NullWritable key, // Input key type
            Iterable<Text> values, // Input value type
            Context context) throws IOException, InterruptedException {
        
        global_max = -1;
        ArrayList<String> global_max_countries = new ArrayList<String>();

        for (Text value: values){
            String[] fields = value.toString().split(",");
            String local_country_max = fields[0];
            int local_max = Integer.parseInt(fields[1]);
            if (local_max > global_max){
                global_max_countries.clear();
                global_max_countries.add(local_country_max);
                global_max = local_max;
            } else if (local_max == global_max){
                global_max_countries.add(local_country_max);
            }
        }

        for (String s: global_max_countries){
            context.write(new Text(s), new IntWritable(global_max));
        }
    }

}
