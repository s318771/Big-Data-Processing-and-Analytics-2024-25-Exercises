// POLISALES
// joboffers.txt format: offer_id, job_id, salary, status, ssn
// jobs with no accepted offers and more than 10 high-salary offers

/*
    strategy:
    - create new class : StatusCounter with values [accepted_count, filtered_rejected_count]
    mapper1: (buffer, file_line) -> (job_id__salary, StatusCounter{accepted_count = 0/1, filtered_rejected_count = 0/1})
    reducer1: (job_id, [StatusCounter{accepted_count = 0/1, filtered_rejected_count = 0/1}, ..])
            compute for each job_id : tot_accepted_count and tot_filtered_rejected_count
            emit (job_id, salary)
            
*/

// SBAGLIATO, NON VUOLE IL SALARY, PRODUCE ANCHE CONCETTUALMENTE UN RISULTATO DIVERSO DA QUELLO SPERATO PERCHè 
// IL GROUP BY PRIMA DEL REDUCER VIENE FATTO NON SU job_id MA SULLA COPPIA job_id, salary :(

public class StatusCounter implements Writable{
    int accepted;
    int rejected;

    // getters, setters...

    @Override
    public void readFields(DataInput in) throws IOException{
        accepted = in.readInt();
        rejected = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(accepted);
        out.writeInt(rejected);
    }
}

public class MapperBigData extends Mapper<
        LongWritable, 
        Text, 
        Text, 
        StatusCounter> {

    // map method
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // joboffers.txt format: offer_id, job_id, salary, status, ssn
        String[] fields = value.toString().split(",");
        String job_id = fields[1] 
        Double salary = Double.parseDouble(fields[2]);
        String status = fields[3];

        StatusCounter statusCounter = new StatusCounter();

        if(status.equals("Accepted")){
            statusCounter.setAccepted(1);
            context.write(new Text(job_id+"_"+salary), statusCounter); 
        } else{
            if (salary > 100*1000){
                statusCounter.setRejected(1);
                context.write(new Text(job_id+"_"+salary), statusCounter);
            }
        }
    }
}


class ReducerBigData extends Reducer<Text, // Input key type
        StatusCounter, // Input value type
        Text, // Output key type
        DoubleWritable> { // Output value type

    // reduce method
    @Override
    protected void reduce(
            Text key, // Input key type
            Iterable<StatusCounter> values, // Input value type
            Context context) throws IOException, InterruptedException {
        
        int countAccepted = 0; 
        int countRejected = 0;

        for (StatusCounter value: values){
            countAccepted += value.getAccepted();
            countRejected += value.getRejected();
        }

        if (countAccepted == 0 && countRejected >= 10){
            String[] fields = key.toString().split("_");
            String job_id = fields[0];
            double salary = Double.parseDouble(fields[1]);
            context.write(new Text(job_id), new DoubleWritable(salary));
        }

    }
}