package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer1 - Counting rejected and accepted offers per job ID
 * Receives as input:
 * (jobID, [1,0]) or
 * (jobID, [0,1])
 * with an offerStatusCounter object that counts the number of rejected and accepted offers.
 */
class Reducer1 extends Reducer<
                Text,           // Input key type
                OfferStatusCounter,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type
    
    @Override
    
    protected void reduce(
        Text key, // Input key type
        Iterable<OfferStatusCounter> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // Iterate over the set of values and sum them
        int sumAccepted = 0;
        int sumRejected = 0;
        for (OfferStatusCounter value : values) {
            sumAccepted += value.getAccepted();
            sumRejected += value.getRejected();
        }

        if (sumAccepted == 0 && sumRejected >= 10) {
            context.write(
                key,                       // Job ID
                new Text(""+sumRejected)); // Number of high-salary rejected offers 
        }
    }
}
