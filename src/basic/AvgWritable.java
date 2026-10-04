package basic;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgWritable implements Writable {
    private double soma;
    private long n;

    public AvgWritable() {}  // construtor vazio é obrigatório

    public AvgWritable(double soma, long n) {
        this.soma = soma;
        this.n = n;
    }

    public double getSoma() { return soma; }
    public long getN() { return n; }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(n);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        n = in.readLong();
    }

    @Override
    public String toString() {
        return soma + "," + n;
    }
}