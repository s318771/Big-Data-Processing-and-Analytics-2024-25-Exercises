public class MapperBigData extends Mapper<
        LongWritable, 
        Text, 
        ??, 
        ??> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}


class ReducerBigData extends Reducer<??, // Input key type
        ??, // Input value type
        ??, // Output key type
        ??> { // Output value type

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<??> values, // Input value type
            Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}


class MapperBigData2 extends Mapper<Text, // Input key type
        Text, // Input value type
        ??, // Output key type
        ??> { // Output value type

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    // map method
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}

public class ReducerBigData2 extends Reducer<??, ??, ??, ??> {

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

    }

    // reduce method
    @Override
    protected void reduce(
            ?? key, // Input key type
            Iterable<??> values, // Input value type
            Context context) throws IOException, InterruptedException {

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {

    }
}
