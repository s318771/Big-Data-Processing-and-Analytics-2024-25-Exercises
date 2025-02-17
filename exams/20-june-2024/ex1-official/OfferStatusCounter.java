package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OfferStatusCounter implements org.apache.hadoop.io.Writable {

	private int accepted;
	private int rejected;

    public int getAccepted() {
        return accepted;
    }

    public int getRejected() {
        return rejected;
    }

    public void setAccepted(int accepted) {
        this.accepted = accepted;
    }

    public void setRejected(int rejected) {
        this.rejected = rejected;
    }

	@Override
	public void readFields(DataInput in) throws IOException {
		accepted = in.readInt();
		rejected = in.readInt();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(accepted);
		out.writeInt(rejected);

	}

}