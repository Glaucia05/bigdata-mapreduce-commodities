package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class CategoryCount {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // arquivo de saida
        Path output = new Path("output/CategoryCount.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "CategoryCount");

        // registro das classes
        j.setJarByClass(CategoryCount.class);
        j.setMapperClass(MapForCategoryCount.class);
        j.setReducerClass(ReduceForCategoryCount.class);

        // definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForCategoryCount extends Mapper<LongWritable, Text, Text, IntWritable> {

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

            // Verificar se existem as 10 colunas
            if (itens.length != 10) {
                return;
            }

            // Ignorar o cabeçalho
            if (itens[0].trim().equalsIgnoreCase("country_or_area")) {
                return;
            }

            // Obter o categoria
            String categoria = itens[9].trim();

            // Emitir o categoria e a contagem
            con.write(new Text(categoria), new IntWritable(1));

        }
    }

    public static class ReduceForCategoryCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        // Funcao de reduce
        public void reduce(Text key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            int soma = 0;

            for(IntWritable v: values){
                soma += v.get();
            }

            con.write(key, new IntWritable(soma));
        }
    }
}
