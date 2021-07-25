import json
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

# Spark Session
spark = SparkSession \
    .builder \
    .appName("Cognitivo.ai") \
    .getOrCreate()


def extract_data(path=""):
    return spark.read.csv(path, sep=',', header=True, inferSchema=True)


# Solução 2: Deduplicação dos dados convertidos: No conjunto de dados convertidos haverão múltiplas entradas para
# um mesmo registro, variando apenas os valores de alguns dos campos entre elas. Será necessário realizar
# um processo de deduplicação destes dados, a fim de apenas manter a última entrada de cada registro,
# usando como referência o id para identificação dos registros duplicados e a data de atualização (update_date)
# para definição do registro mais recente;
def reduce_data(dataframe):
    window = Window.partitionBy(col('id')).orderBy(col('update_date').desc())
    return dataframe.withColumn("row_number", row_number().over(window)).filter(col('row_number') == 1) \
        .drop(col("ranked_id")).sort(col('id').asc())

#Solução 3: Conversão do tipo dos dados deduplicados: No diretório config haverá um arquivo JSON de configuração
# (types_mapping.json), contendo os nomes dos campos e os respectivos tipos desejados de output.
# Utilizando esse arquivo como input, realizar um processo de conversão dos tipos dos campos descritos,
# no conjunto de dados deduplicados;
def apply_file_configs(dataframe, config_file_path):
    with open(config_file_path) as config_maps:
        configs = json.load(config_maps)
        for column in configs:
            dataframe = dataframe.withColumn(column, col(column).cast(configs[column]))
    return dataframe


def main():
    path_to_read = "./data/input/users/load.csv"
    path_to_config = "./config/types_mapping.json"
    path_to_save = "./data/output/"

    # Extract Data
    extracted_data = extract_data(path_to_read)

    # Solução 3
    config_dataframe = apply_file_configs(extracted_data, path_to_config)

    # Solução 2
    reduced_data = reduce_data(config_dataframe)

    # Solução 1: Conversão do formato dos arquivos: Converter o arquivo CSV presente no diretório
    # data/input/users/load.csv, para um formato colunar de alta performance de leitura de sua escolha.
    # Justificar brevemente a escolha do formato;

    # Justificativa:
    # Foi escolhido o formato parquet por ele ser otimizado para leitura e também ser bastante agnóstico quanto
    # ao ambiente, podendo rodar no hdfs, ou em ambientes cloud facilmente.
    reduced_data.write.parquet(path_to_save, mode='overwrite')



if __name__ == '__main__':
    main()