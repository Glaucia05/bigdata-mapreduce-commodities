package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class MaxBrazil {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo intermediario (saida do job 1 e entrada do job 2)
        Path intermediate = new Path("output/MaxBrazilTemp.txt");

        // arquivo de saida final
        Path output = new Path("output/MaxBrazil.txt");


        Job j1 = new Job(c, "maxbrazil-job1");

        j1.setJarByClass(MaxBrazil.class);
        j1.setMapperClass(MapForMax.class);
        j1.setCombinerClass(ReduceForMax.class);
        j1.setReducerClass(ReduceForMax.class);

        j1.setMapOutputKeyClass(Text.class);
        j1.setMapOutputValueClass(DoubleWritable.class);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);


        if (!j1.waitForCompletion(true)) {
            System.exit(1);
        }


        Job j2 = new Job(c, "maxbrazil-job2");

        j2.setJarByClass(MaxBrazil.class);
        j2.setMapperClass(MapForSort.class);
        j2.setReducerClass(ReduceForSort.class);
        j2.setSortComparatorClass(DescendingDoubleComparator.class);

        j2.setMapOutputKeyClass(DoubleWritable.class);
        j2.setMapOutputValueClass(Text.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        System.exit(j2.waitForCompletion(true) ? 0 : 1);
    }



    public static class MapForMax extends Mapper<LongWritable, Text, Text, DoubleWritable> {

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

            // Se for Brasil, manda (ano, trade_usd) para o reduce
            if (itens[0].trim().equals("Brazil")) {
                try {
                    Text chave = new Text(itens[1].trim());
                    DoubleWritable valor = new DoubleWritable(Double.parseDouble(itens[5].trim()));
                    con.write(chave, valor);
                } catch (NumberFormatException e) {
                    // ignora trade_usd vazio ou invalido
                }
            }
        }
    }

    public static class ReduceForMax extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        // Funcao de reduce (tambem serve de combiner)
        public void reduce(Text key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            double max = Double.NEGATIVE_INFINITY;

            for (DoubleWritable v : values) {
                if (v.get() > max) {
                    max = v.get();
                }
            }

            con.write(key, new DoubleWritable(max));
        }
    }



    public static class MapForSort extends Mapper<LongWritable, Text, DoubleWritable, Text> {

        // Funcao de map: le "ano<TAB>max" e inverte para (max, ano)
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String[] campos = value.toString().split("\t");

            if (campos.length != 2) {
                return;
            }

            con.write(new DoubleWritable(Double.parseDouble(campos[1])), new Text(campos[0]));
        }
    }

    public static class ReduceForSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable> {

        // Funcao de reduce: desfaz a inversao, ja chega ordenado
        public void reduce(DoubleWritable key, Iterable<Text> values, Context con)
                throws IOException, InterruptedException {

            for (Text ano : values) {
                con.write(ano, key);
            }
        }
    }

    // Comparador que inverte a ordem (maior primeiro)
    public static class DescendingDoubleComparator extends DoubleWritable.Comparator {

        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }
}