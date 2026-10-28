# Big Data – Análise de Transações Comerciais com Hadoop MapReduce

Projeto da disciplina de **Big Data** (Bacharelado em Ciência da Computação – PUCPR), ministrada pelo Prof. Joelton Deonei Gotz.

O objetivo é usar o modelo de programação **MapReduce** (Hadoop, em Java) para extrair informações de um dataset com mais de **8 milhões de transações comerciais internacionais** realizadas ao longo de 30 anos.

---

## Dataset

Arquivo CSV com colunas separadas por ponto e vírgula (`;`) e 10 colunas por linha:

| Coluna | Descrição |
|---|---|
| Country | País envolvido na transação |
| Year | Ano em que a transação ocorreu |
| Commodity code | Identificador da mercadoria |
| Commodity | Descrição da mercadoria |
| Flow | Tipo de fluxo (Export ou Import) |
| Price | Preço, em USD |
| Weight | Peso da mercadoria |
| Unit | Unidade de medida (ex.: Number of items) |
| Amount | Quantidade na unidade informada |
| Category | Categoria da mercadoria (ex.: Live animals) |

Exemplo de linha:

```
Afghanistan;2016;010410;Sheep, live;Export;6088;2339;Number of items;51;01_live_animals
```

> O dataset não está incluído neste repositório por causa do tamanho.

---

## Problemas resolvidos

| # | Questão | Requisitos obrigatórios |
|---|---|---|
| 1 | Número de transações envolvendo o Brasil | – |
| 2 | Número de transações por ano | – |
| 3 | Número de transações por categoria | – |
| 4 | Número de transações por tipo de fluxo (flow) | – |
| 5 | Valor médio das transações por ano, somente no Brasil | Writable customizado |
| 6 | Transação mais cara e mais barata no Brasil em 2016 | Combiner + Writable customizado |
| 7 | Valor médio das transações por ano, somente exportações do Brasil | Combiner + Writable customizado |
| 8 | Valor máximo das transações por ano no Brasil, ordenado do maior para o menor | Concatenação de Jobs |
| 9 | Transação de maior e menor preço (coluna *amount*) por ano e país | Comparable Writable + Combiner |

---

## Regras de implementação

- **Cabeçalho:** a primeira linha do CSV é descartada no Mapper.
- **Dados faltantes:** linhas com número de colunas diferente de 10 ou com campos numéricos vazios/inválidos são ignoradas.
- **Chaves e valores compostos:** são feitos com classes `Writable` / `WritableComparable` próprias, **sem concatenação de strings**.

---

## Tecnologias

- Java 17 (Eclipse Temurin)
- Apache Hadoop 3.2.1
- Maven / IntelliJ IDEA

> O Hadoop 3.2.1 não é compatível com JDKs mais novos (como 25/26), por isso o projeto usa o JDK 17.

---

## Estrutura do projeto

```
big-data-mapreduce-pjbl1/
├── src/main/java/
│   └── basic/              # Classes dos exercícios (Mapper, Reducer, Combiner, Driver)
│                           # e Writables customizados (ex.: AvgWritable)
├── resultados/             # Saída de cada exercício em arquivo .txt
├── pom.xml
└── README.md
```

> Ajuste os nomes das pastas e classes acima conforme a estrutura real do seu repositório.

---

## Como executar

1. Gere o JAR do projeto:

   ```bash
   mvn clean package
   ```

2. Envie o dataset para o HDFS (ou use o sistema de arquivos local, em modo standalone):

   ```bash
   hdfs dfs -put transactions.csv /input/
   ```

3. Execute o job desejado:

   ```bash
   hadoop jar target/big-data-mapreduce-pjbl1.jar basic.NomeDaClasse /input/transactions.csv /output/nome-do-exercicio
   ```

   Substitua `NomeDaClasse` pela classe do exercício (por exemplo, `basic.YearCount`).

4. Consulte o resultado:

   ```bash
   hdfs dfs -cat /output/nome-do-exercicio/part-r-00000
   ```

   > O diretório de saída não pode existir antes da execução.

---

## Resultados

A saída de cada exercício está salva em arquivo `.txt` separado, na pasta `resultados/`.

---
