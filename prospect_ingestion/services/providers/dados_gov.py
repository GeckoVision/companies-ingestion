from datasets.empresas import dados_empresas
from datasets.estabelecimentos import dados_estabelecimentos
from datasets.socios import dados_socios
from utils.data_cleansing import DataCleansing
import fnmatch
from pyspark.sql.functions import *
from pyspark.sql.types import *
import glob

class Dados_govProvider:
    """
       Implementacao do pipeline de ingestao dos dados provenientes do dados gov
    """

    def __init__(self, spark, df_utils):
        super().__init__()
        self.df_utils = df_utils
        self.log_process = df_utils(spark).log_process
        self.spark = spark
        self.provider_name = "dados_gov"

    def build_dados_empresa(self, df_dados_empresa):

        self.log_process("[PASSO 2] - Formatando dataframe dados empresa !")

        path = "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_empresas/"
        df_dados_empresa.write.mode("overwrite").parquet(f"{path}")
        df_dados_empresa = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_empresa

    def build_dados_estabelecimento(self, df_dados_estabelecimento):

        self.log_process("[PASSO 3] - Formatando dataframe dados estabelecimento !")

        path = "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_estabelecimentos/"
        df_dados_estabelecimento.write.mode("overwrite").parquet(f"{path}")
        df_dados_estabelecimento = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_estabelecimento

    def build_dados_socios(self, df_dados_socios):

        self.log_process("[PASSO 4] - Formatando dataframe dados socios !")

        path = "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_socios/"
        df_dados_socios.write.mode("overwrite").parquet(f"{path}")
        df_dados_socios = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_socios

    def build_intermediario(self):

        df_empresas = self.spark.read.parquet(
            "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_empresas/")
        df_estabelecimentos = self.spark.read.parquet(
            "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_estabelecimentos/")
        df_socios = self.spark.read.parquet(
            "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_socios/")

        df_empresas.createOrReplaceTempView("empresas")
        df_estabelecimentos.createOrReplaceTempView("estabelecimentos")
        df_socios.createOrReplaceTempView("socios")

        result_intermed = self.spark.sql("""
        SELECT 
        emp.cnpj_basico,
        emp.RAZAO_SOC,
        
        date_format(current_date(), 'y-MM') as dat_ref_carga
        FROM empresas emp
        LEFT JOIN estabelecimentos e 
         ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN socios s
         ON e.cnpj_basico = s.cnpj_basico
        """)

        result_intermed.write.mode("overwrite").parquet("/tmp/result_intermed")
        result_intermed = self.spark.read.parquet("/tmp/result_intermed")
        result_intermed.show(truncate=False)

        result_intermed.createOrReplaceTempView("result_intermed")


    def build_dfs(self):
        df_names = ['Dados_Empresas', 'Dados_Estabelecimentos', 'Dados_Socios']
        schema_names = [dados_empresas, dados_estabelecimentos, dados_socios]

        dir = '/home/nan/gecko/datasets/'

        files = ['Empresas/*', 'Estabelecimentos/*', 'Socios/*']

        for i in range(0, len(schema_names)):
            schema = schema_names[i]
            file = f'{dir}{files[i]}'
            df_name = df_names[i]
            globals()[f"df_{df_name}"] = self.df_utils(self.spark).define_dfs(schema, file, ";")

    def run(self):
        DataCleansing(self.spark).main()

        self.log_process("[PASSO 1] - preparando dataframes !")
        self.build_dfs()
        self.log_process("[PASSO 2] - gravando dados de empresa !")
        self.build_dados_empresa(df_dados_empresa=df_Dados_Empresas)
        self.log_process("[PASSO 3] - gravando dados de estabelecimentos !")
        self.build_dados_estabelecimento(df_dados_estabelecimento=df_Dados_Estabelecimentos)
        self.log_process("[PASSO 4] - gravando dados de socios !")
        self.build_dados_socios(df_dados_socios=df_Dados_Socios)
