package it.polito.bigdata.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CityCount implements org.apache.hadoop.io.Writable {
    public String city;
    public int count;


    public String getCity() {
        return city;
    }
    public void setCity(String city) {
        this.city = city;
    }
    public int getCount() {
        return count;
    }
    public void setCount(int count) {
        this.count = count;
    }

    public String toString() {
        return city + "," + count;
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(city);
        out.writeInt(count);
    }
    
    @Override
    public void readFields(DataInput in) throws IOException {
        city = in.readUTF();
        count = in.readInt();
    }
}
