package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AvgExpBrazil {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/brazilAvgExpo.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "avgexpbrazil");

        // registro das classes
        j.setJarByClass(AvgExpBrazil.class);
        j.setMapperClass(MapForBrazilExpAvg.class);
        j.setCombinerClass(CombineForBrazilExpAvg.class);
        j.setReducerClass(ReduceForBrazilExpAvg.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(AvgWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForBrazilExpAvg extends Mapper<LongWritable, Text, Text, AvgWritable> {

        // Funcao de map (igual ao seu)
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
            String[] colunas = value.toString().split(";");

            if (colunas[0].equals("Brazil") && colunas[4].equals("Export")) {
                String ano = colunas[1].trim();
                double valor = Double.parseDouble(colunas[5].trim());
                con.write(new Text(ano), new AvgWritable(valor, 1));
            }
        }
    }


    public static class CombineForBrazilExpAvg extends Reducer<Text, AvgWritable, Text, AvgWritable> {
        public void reduce(Text key, Iterable<AvgWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            long n = 0;

            for (AvgWritable v : values) {
                soma += v.getSoma();
                n += v.getN();
            }

            con.write(key, new AvgWritable(soma, n));
        }
    }


    public static class ReduceForBrazilExpAvg extends Reducer<Text, AvgWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<AvgWritable> values, Context con)
                throws IOException, InterruptedException {

            double soma = 0;
            long n = 0;

            for (AvgWritable v : values) {
                soma += v.getSoma();
                n += v.getN();
            }

            con.write(key, new DoubleWritable(soma / n));
        }
    }
}