
        package basic;

// Importa a interface Writable do Hadoop, que permite serializar
// os dados para que possam ser armazenados e transferidos.
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AvgWritable implements Writable {

    // Guarda a soma dos valores das exportações.
    private double soma;

    // Guarda a quantidade de registros considerados.
    private long n;

    // Construtor vazio obrigatório para o Hadoop criar o objeto
    // antes de preencher seus atributos durante a leitura.
    public AvgWritable() {}

    // Construtor que recebe a soma e a quantidade
    // e atribui esses valores aos atributos do objeto.
    public AvgWritable(double soma, long n) {
        this.soma = soma;
        this.n = n;
    }

    // Retorna a soma armazenada no objeto.
    public double getSoma() {
        return soma;
    }

    // Retorna a quantidade de registros armazenada no objeto.
    public long getN() {
        return n;
    }

    // Escreve os atributos do objeto em um fluxo de saída.
    // Primeiro escreve a soma como double e depois a quantidade como long.
    // A ordem deve ser mantida também no método readFields().
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeDouble(soma);
        out.writeLong(n);
    }

    // Lê os dados de um fluxo de entrada e preenche os atributos do objeto.
    // Primeiro lê a soma e depois a quantidade, na mesma ordem do write().
    @Override
    public void readFields(DataInput in) throws IOException {
        soma = in.readDouble();
        n = in.readLong();
    }

    // Define como o objeto será representado quando convertido em texto.
    // Retorna a soma e a quantidade separadas por uma vírgula.
    // Exemplo: 3500.0,3
    @Override
    public String toString() {
        return soma + "," + n;
    }
}
