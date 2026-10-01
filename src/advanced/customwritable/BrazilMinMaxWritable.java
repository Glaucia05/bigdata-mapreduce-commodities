
package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class BrazilMinMaxWritable implements Writable {

    // 1. Atributos privados
    private float minimo;
    private float maximo;

    // 2. Getters e setters
    public float getMinimo() {
        return minimo;
    }

    public void setMinimo(float minimo) {
        this.minimo = minimo;
    }

    public float getMaximo() {
        return maximo;
    }

    public void setMaximo(float maximo) {
        this.maximo = maximo;
    }

    // 3. Construtor vazio
    public BrazilMinMaxWritable() {
    }

    // 4. Construtor com parâmetros
    public BrazilMinMaxWritable(float minimo, float maximo) {
        this.minimo = minimo;
        this.maximo = maximo;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(minimo));
        out.writeUTF(String.valueOf(maximo));
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        minimo = Float.parseFloat(in.readUTF());
        maximo = Float.parseFloat(in.readUTF());
    }

    @Override
    public String toString() {
        return "Minimo: " + minimo + "\tMaximo: " + maximo;
    }
}
