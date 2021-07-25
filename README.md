# Cognitivo.ai
Resolução para o processo seletivo cognitivo.ai

### Solução 1
Foi selecionado o formato `parquet` por ele ser otimizado para leitura e fácil de ser utilizado em qualquer ambiente, Cloudera 
ou ambientes na nuvem como AWS, GCP, Azure.

### Execução 
Eu executaria o job na nuvem `EMR` pois o ambiente é otimizado para o Spark e já integra os dados no S3 para leitura, 
além de permitir também que o parquet seja utilizado com o Redshift. 