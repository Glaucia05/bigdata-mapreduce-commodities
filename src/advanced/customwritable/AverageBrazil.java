
package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageBrazil {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args)
                .getRemainingArgs();

        // Arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // Arquivo de saída
        Path output = new Path("output/AverageBrazil.txt");

        // Criação do Job
        Job j = new Job(c, "averagebrazil");

        // 1. Registrar classes
        j.setJarByClass(AverageBrazil.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // 2. Definir tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(BrazilAvgWritable.class);

        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // 3. Cadastrar arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // 4. Lançar o Job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class MapForAverage extends
            Mapper<LongWritable, Text, Text, BrazilAvgWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();

            // Separar as colunas pelo ponto e vírgula
            String[] col = linha.split(";", -1);

            // Verificar quantidade de colunas
            if (col.length != 10) {
                return;
            }

            // Ignorar cabeçalho
            if (col[0].trim().equalsIgnoreCase("country_or_area")) {
                return;
            }

            // Considerar somente transações do Brasil
            if (!col[0].trim().equalsIgnoreCase("Brazil")) {
                return;
            }

            // Tratar dados faltantes
            if (col[1].trim().isEmpty() ||
                    col[5].trim().isEmpty()) {
                return;
            }

            try {
                String ano = col[1].trim();
                float preco = Float.parseFloat(col[5].trim());

                // Ignorar preços inválidos
                if (!Float.isFinite(preco)) {
                    return;
                }

                Text chave = new Text(ano);

                BrazilAvgWritable valor = new BrazilAvgWritable(preco, 1);

                con.write(chave, valor);

            } catch (NumberFormatException e) {
                // Ignorar registros inválidos
            }
        }
    }

    public static class ReduceForAverage extends
            Reducer<Text, BrazilAvgWritable,
                    Text, FloatWritable> {

        public void reduce(Text key,
                           Iterable<BrazilAvgWritable> values,
                           Context con)
                throws IOException, InterruptedException {

            float somaP = 0;
            int somaF = 0;

            for (BrazilAvgWritable v : values) {
                somaP += v.getPreco();
                somaF += v.getFreq();
            }

            if (somaF > 0) {
                float media = somaP / somaF;

                con.write(key, new FloatWritable(media));
            }
        }
    }
}
