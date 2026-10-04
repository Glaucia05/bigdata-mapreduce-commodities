package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Locale;

public class MinMaxCountryYear {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/MinMaxCountryYear.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "minmaxcountryyear");

        // registro das classes
        j.setJarByClass(MinMaxCountryYear.class);
        j.setMapperClass(MapForMinMax.class);
        j.setCombinerClass(ReduceForMinMax.class);
        j.setReducerClass(ReduceForMinMax.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(CountryYearWritable.class);
        j.setMapOutputValueClass(MinMaxWritable.class);
        j.setOutputKeyClass(CountryYearWritable.class);
        j.setOutputValueClass(MinMaxWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class CountryYearWritable implements WritableComparable<CountryYearWritable> {
        private String pais;
        private String ano;

        public CountryYearWritable() {}  // construtor vazio e obrigatorio

        public CountryYearWritable(String pais, String ano) {
            this.pais = pais;
            this.ano = ano;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(pais);
            out.writeUTF(ano);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            pais = in.readUTF();
            ano = in.readUTF();
        }


        @Override
        public int compareTo(CountryYearWritable o) {
            int r = pais.compareTo(o.pais);
            if (r != 0) {
                return r;
            }
            return ano.compareTo(o.ano);
        }


        @Override
        public int hashCode() {
            return pais.hashCode() * 163 + ano.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof CountryYearWritable)) {
                return false;
            }
            CountryYearWritable o = (CountryYearWritable) obj;
            return pais.equals(o.pais) && ano.equals(o.ano);
        }

        @Override
        public String toString() {
            return pais + "    " + ano;
        }
    }

    // Valor customizado: menor e maior valor
    public static class MinMaxWritable implements Writable {
        private double min;
        private double max;

        public MinMaxWritable() {}  // construtor vazio e obrigatorio

        public MinMaxWritable(double min, double max) {
            this.min = min;
            this.max = max;
        }

        public double getMin() { return min; }
        public double getMax() { return max; }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeDouble(min);
            out.writeDouble(max);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            min = in.readDouble();
            max = in.readDouble();
        }

        @Override
        public String toString() {
            return String.format(Locale.US, "min=%.2f max=%.2f", min, max);
        }
    }

    public static class MapForMinMax extends Mapper<LongWritable, Text, CountryYearWritable, MinMaxWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString().trim();

            // Ignorar linhas vazias
            if (linha.isEmpty()) {
                return;
            }

            // Separar as colunas do CSV
            String[] itens = linha.split(";", -1);

            // Ignorar o cabeçalho
            if (itens[0].trim().equalsIgnoreCase("country_or_area")) {
                return;
            }

            // Verificar se existem as 10 colunas
            if (itens.length != 10) {
                return;
            }

            try {
                CountryYearWritable chave = new CountryYearWritable(itens[0].trim(), itens[1].trim());
                double valor = Double.parseDouble(itens[5].trim());
                // cada transacao e, sozinha, o menor e o maior
                con.write(chave, new MinMaxWritable(valor, valor));
            } catch (NumberFormatException e) {
                // ignora trade_usd vazio ou invalido
            }
        }
    }

    // Serve de Combiner e de Reducer (mesmos tipos de entrada e saida)
    public static class ReduceForMinMax extends Reducer<CountryYearWritable, MinMaxWritable, CountryYearWritable, MinMaxWritable> {

        // Funcao de reduce
        public void reduce(CountryYearWritable key, Iterable<MinMaxWritable> values, Context con)
                throws IOException, InterruptedException {

            double min = Double.POSITIVE_INFINITY;
            double max = Double.NEGATIVE_INFINITY;

            for (MinMaxWritable v : values) {
                if (v.getMin() < min) {
                    min = v.getMin();
                }
                if (v.getMax() > max) {
                    max = v.getMax();
                }
            }

            con.write(key, new MinMaxWritable(min, max));
        }
    }
}