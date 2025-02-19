

public class Counter2021 implements org.apache.hadoop.io.Writable{
    private int counter20 = 0;
    private int counter21 = 0;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(counter20);
        out.writeInt(counter21);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        counter20 = in.readInt();
        counter21 = in.readInt();
    }

    public int getCounter20() {
        return counter20;
    }

    public void setCounter20(int counter20) {
        this.counter20 = counter20;
    }

    public int getCounter21() {
        return counter21;
    }

    public void setCounter21(int counter21) {
        this.counter21 = counter21;
    }
}