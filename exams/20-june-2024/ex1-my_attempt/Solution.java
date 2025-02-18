// JobOffers.txt: offer_id, job_id, salary, status, ssn
// Jobs with no accepted offers and more than 10 high-salary rejected offers

public class StatusCounter implements org.apache.hadoop.io.Writable{
    int accepted;
    int rejected;

    // per getter e setter è specificatamente detto di non implementarli :) 

    // Questi metodi sono utilizzati per la serializzazione e deserializzazione degli oggetti, 
    // ovvero per leggere e scrivere i dati da e verso un flusso di input/output.
    @Override
    public readFields(DataInput in) throws IOException{
        accepted = in.readInt(); // legge un intero dal flusso di input e lo assegna alla variabile accepted
        rejected = in.readInt();
    }
    @Override
    public write(DataOutput out) throws IOException{
        out.writeInt(accepted);
        out.writeInt(rejected);
    }
} 

public class MapperBigData extends Mapper<
                LongWritable, 
                Text, 
                Text, 
                OfferStatusCounter> {

    private final static int salaryThreshold = 100 * 1000; // cento mila, più leggibile rispetto a 100000

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // JobOffers.txt: offer_id, job_id, salary, status, ssn
        String[] fields = value.toString().split(',');
        
        String jobId = fields[0];
        double salary = Double.parseDouble(fields[2]);
        String status = fields[3];

        OfferStatusCounter offerStatusCounter = new OfferStatusCounter();

        // counting accepted offers
        if (status.equals("Accepted")){
            offerStatusCounter.setAccepted(1);
            context.write(new Text(jobID), offerStatusCounter)
        } else if (status.equals("Rejected") && salary > salaryThreshold){
            offerStatusCounter.setRejected(1);
            context.write(new Text(jobId), offerStatusCounter)
        }


    }
}



class ReducerBigData extends Reducer<Text, // Input key type
        OfferStatusCounter, // Input value type
        Text, // Output key type
        Text> { // Output value type

        // reduce method
        @Override
        protected void reduce(
                Text key, // Input key type
                Iterable<OfferStatusCounter> values, // Input value type
                Context context) throws IOException, InterruptedException {
            
            int sumAccepted = 0;
            int sumRejected = 0;

            for (OfferStatusCounter value : values){
                sumAccepted += value.getAccepted();
                sumRejected += value.getRejected();
            }

            if(sumAccepted == 0 && sumRejected >= 10){
                context.write(
                    key, // jobId
                    new Text(""+sumRejected) // number of high-salary rejected offers
                )
            }

        }
}