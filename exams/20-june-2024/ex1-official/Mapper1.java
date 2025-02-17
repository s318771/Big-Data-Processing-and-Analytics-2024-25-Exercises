package it.polito.bigdata.hadoop;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
Process JobOffers.txt to count
-  rejected offers with a salary higher than 100,000 euros --> these must be more than 10
-  accepted offers --> these must be zero
Emits (jobID, [rejectedCount,acceptedCount]) to count in the reducer.
 */

public class Mapper1 extends Mapper<LongWritable, Text, Text, OfferStatusCounter> {
    
    private final static int salaryThreshold = 100 * 1000;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] fields = value.toString().split(",");
        
        // optional: skip the header (if any)
        if (fields[0].equals("OfferID")) {
            return;
        }
        
        String jobId = fields[1];
        double salary = Double.parseDouble(fields[2]);
        String status = fields[3];

        OfferStatusCounter offerStatusCounter = new OfferStatusCounter();

        // counting accepted offers
        if (status.equals("Accepted")) {
            // System.out.println("Mapper 1 emitting: " + jobId + " accepted");
            offerStatusCounter.setAccepted(1);
            context.write(
                new Text(jobId), 
                offerStatusCounter);
        }
        // counting rejected offers with salary > 100k euros
        else if (status.equals("Rejected") && salary > salaryThreshold) {
            // System.out.println("Mapper 1 emitting: " + jobId + " rejected");
            offerStatusCounter.setRejected(1);
            context.write(
                new Text(jobId), 
                offerStatusCounter);
        }
    }
}
