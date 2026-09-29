
package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BrazilAvgWritable implements Writable {

    // 1. Atributos privados
    private float preco;
    private int freq;

    // 2. Getters e setters
    public float getPreco() {
        return preco;
    }

    public void setPreco(float preco) {
        this.preco = preco;
    }

    public int getFreq() {
        return freq;
    }

    public void setFreq(int freq) {
        this.freq = freq;
    }

    // 3. Construtor vazio
    public BrazilAvgWritable() {
    }

    // 4. Construtor com parâmetros
    public BrazilAvgWritable(float preco, int freq) {
        this.preco = preco;
        this.freq = freq;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(preco));
        out.writeUTF(String.valueOf(freq));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        preco = Float.parseFloat(in.readUTF());
        freq = Integer.parseInt(in.readUTF());
    }
}
