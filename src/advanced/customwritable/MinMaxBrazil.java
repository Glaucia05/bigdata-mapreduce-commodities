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

public class MinMaxBrazil {

    public static void main(String args[]) throws IOException,
            ClassNotFoundException,
            InterruptedException {

        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args)
                .getRemainingArgs();

        // Arquivo de entrada
        Path input = new Path("in/operacoes_comerciais_inteira.csv");

        // Diretório de saída
        Path output = new Path("output/MinMaxBrazil.txt");

        // Criação do Job
        Job j = new Job(c, "minmaxbrazil");

        // 1. Registrar classes
        j.setJarByClass(MinMaxBrazil.class);
        j.setMapperClass(MapForMinMax.class);
        j.setCombinerClass(CombineForMinMax.class);
        j.setReducerClass(ReduceForMinMax.class);

        // 2. Definir tipos de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(BrazilMinMaxWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        // 3. Cadastrar entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // 4. Executar o Job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    // MAPPER
    public static class MapForMinMax extends
            Mapper<LongWritable, Text, Text, BrazilMinMaxWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            String linha = value.toString();
            String[] itens = linha.split(";", -1);

            // Verificar quantidade de colunas
            if (itens.length != 10) {
                return;
            }

            // Ignorar cabeçalho
            if (itens[0].trim().equalsIgnoreCase("country_or_area")) {
                return;
            }

            // Filtrar Brasil e ano de 2016
            if (!itens[0].trim().equalsIgnoreCase("Brazil") ||
                    !itens[1].trim().equals("2016")) {
                return;
            }

            // Tratar dados faltantes
            if (itens[5].trim().isEmpty()) {
                return;
            }

            try {
                float preco = Float.parseFloat(itens[5].trim());

                if (!Float.isFinite(preco)) {
                    return;
                }

                Text chave = new Text("Brazil");

                BrazilMinMaxWritable valor = new BrazilMinMaxWritable(preco, preco);

                con.write(chave, valor);

            } catch (NumberFormatException e) {
                // Ignorar valores inválidos
            }
        }
    }

    // COMBINER
    public static class CombineForMinMax extends
            Reducer<Text, BrazilMinMaxWritable,
                    Text, BrazilMinMaxWritable> {

        public void reduce(Text key,
                           Iterable<BrazilMinMaxWritable> values,
                           Context con)
                throws IOException, InterruptedException {

            float minimo = Float.MAX_VALUE;
            float maximo = -Float.MAX_VALUE;

            for (BrazilMinMaxWritable v : values) {

                if (v.getMinimo() < minimo) {
                    minimo = v.getMinimo();
                }

                if (v.getMaximo() > maximo) {
                    maximo = v.getMaximo();
                }
            }

            BrazilMinMaxWritable resultado = new BrazilMinMaxWritable(minimo, maximo);

            con.write(key, resultado);
        }
    }

    // REDUCER
    public static class ReduceForMinMax extends
            Reducer<Text, BrazilMinMaxWritable,
                    Text, Text> {

        public void reduce(Text key,
                           Iterable<BrazilMinMaxWritable> values,
                           Context con)
                throws IOException, InterruptedException {

            float minimo = Float.MAX_VALUE;
            float maximo = -Float.MAX_VALUE;

            for (BrazilMinMaxWritable v : values) {

                if (v.getMinimo() < minimo) {
                    minimo = v.getMinimo();
                }

                if (v.getMaximo() > maximo) {
                    maximo = v.getMaximo();
                }
            }

            BrazilMinMaxWritable resultado = new BrazilMinMaxWritable(minimo, maximo);

            con.write(key, new Text(resultado.toString()));
        }
    }
}
