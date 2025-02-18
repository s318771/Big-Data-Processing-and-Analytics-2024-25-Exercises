public class MapperBigData extends Mapper<
        LongWritable, 
        Text, 
        Text, 
        NullWritable> {

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
                // Input format: saletimestamp, userId, itemId, salePrice
                // I want to emit pairs (userId_itemId, null) for those in the wanted period
                String[] fields = value.toString().split(",");
                String userId = fields[1];
                String itemId = fields[2];
                String date = fields[0].split(",")[0];
                if (date.compareTo("01/01/2020") >= 0 && date.compareTo("31/12/2023")){
                    context.write(new Text(userId + "_" + itemId), NullWritable.get());
                }
    }
}


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
                // context.write(key, new IntWritable(1));
                // FARE SOLO QUESTO E' DECISAMENTE SBAGLIATO PERCHE' IN OUTPUT AVREI (userId_itemId, 1)
                // E IL JOB 2 NON FARA' UN COUNT SENSATO (SARANNO TUTTE COPPIE DISTINTE)
                // DEVO PASSARGLI (userId, 1) PERCHE' COSI' STO DICENDO CHE PER QUEL userId C'E' UN PRODOTTO DISTINTO
                // E NEL JOB DUE POSSO SOMMARE TUTTI I PRODOTTI DISTINTI
                String userId = key.toString().split("_")[0];
                int oneCount = Integer.parseInt(value.toString());
                context.write(new Text(userId), new IntWritable(oneCount));
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
                int oneCount = Integer.parseInt(value.toString());
                context.write(key, new IntWritable(oneCount));

    }
}

public class ReducerBigData2 extends Reducer<Text, IntWritable, Text, NullWritable> {

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<IntWritable> values, // Input value type
            Context context) throws IOException, InterruptedException {
                int final_count = 0;
                for (IntWritable value: values){
                    final_count += value.get();
                }
                if (final_count >= 50){
                    context.write(new Text(key), NullWritable.get())
                }
    }
}
