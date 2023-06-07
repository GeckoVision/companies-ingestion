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
        df_dados_empresa.coalesce(4).write.mode("overwrite").parquet(f"{path}")
        df_dados_empresa = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_empresa

    def build_dados_estabelecimento(self, df_dados_estabelecimento):

        self.log_process("[PASSO 3] - Formatando dataframe dados estabelecimento !")

        path = "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_estabelecimentos/"
        df_dados_estabelecimento.coalesce(4).write.mode("overwrite").parquet(f"{path}")
        df_dados_estabelecimento = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_estabelecimento

    def build_dados_socios(self, df_dados_socios):

        self.log_process("[PASSO 4] - Formatando dataframe dados socios !")

        path = "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_socios/"
        df_dados_socios.coalesce(4).write.mode("overwrite").parquet(f"{path}")
        df_dados_socios = self.spark.read.parquet(f"{path}").withColumn("data_atualizacao", date_format(current_date(), 'y-MM'))

        return df_dados_socios

    def build_intermediario(self):

        df_Dados_Empresas.createOrReplaceTempView("empresas")
        df_Dados_Estabelecimentos.createOrReplaceTempView("estabelecimentos")
        df_Dados_Socios.createOrReplaceTempView("socios")

        result_intermed = self.spark.sql("""
        SELECT 
            emp.cnpj_basico,
            REGEXP_REPLACE(udf_remove_accents(emp.RAZAO_SOC), '[^A-Za-z]', ' ') AS RAZAO_SOC,
            e.NOM_FANTASIA,
            emp.NAT_JURID,
            CONCAT(e.CNPJ_BASICO, "/", e.CNPJ_ORDEM, "-", e.CNPJ_DV) AS CNPJ_COMPLETO,
            e.EMAIL,
            e.PAIS,
            e.UF,
            e.MUNICIPIO,
            e.CNAE_FISC_PRIN,
            e.TIP_LOGRADOURO,
            e.LOGRADOURO,
            e.NUMERO,
            e.BAIRRO,
            e.CEP,
            s.ID_SOCIO,
            REGEXP_REPLACE(udf_remove_accents(s.NOM_SOCIO), '[^A-Za-z]', ' ') AS NOM_SOCIO,
            date_format(current_date(), 'y-MM') as dat_ref_carga
        FROM empresas emp
        INNER JOIN estabelecimentos e 
         ON emp.cnpj_basico = e.cnpj_basico
        LEFT JOIN socios s
         ON e.cnpj_basico = s.cnpj_basico
        WHERE e.SITU_CAD = '02'
              AND e.UF = 'SP'
        """)

        result_intermed.write.mode("overwrite").parquet("/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_prospects/")

    def export_data(self):
        result_intermed = self.spark.read.parquet("/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/dataset_prospects/")
        result_intermed.show(truncate=False)

        result_intermed.createOrReplaceTempView("prospects")

        export = self.spark.sql("""

        WITH CTE_prospects as (
        SELECT
            CNPJ_COMPLETO,
            CASE WHEN LENGTH(NOM_SOCIO) > 1 THEN SPLIT(BTRIM(NOM_SOCIO), ' ')[0]
                 WHEN LENGTH(RAZAO_SOC) > 1 THEN SPLIT(BTRIM(RAZAO_SOC), ' ')[0]
            END AS `first name`,
            email,
            'Brazil' as country,
            CASE WHEN LENGTH(NOM_FANTASIA) > 1  THEN REPLACE(REPLACE(BTRIM(NOM_FANTASIA), 'LTDA', ''), '-', '')
                 WHEN LENGTH(RAZAO_SOC)    > 1  THEN REPLACE(REPLACE(BTRIM(RAZAO_SOC), 'LTDA', ''), '-', '')
            END AS `company name`,
            'Sao Paulo, Sao Paulo, Brazil' as location
        FROM prospects
        WHERE EMAIL IS NOT NULL AND MUNICIPIO="7107" AND CNAE_FISC_PRIN='9609207' )

        SELECT 
               CNPJ_COMPLETO,
               `first name`,
               email,
               country,
               concat( split( `company name`, ' ' )[0], ' ', NVL(split( `company name`, ' ' )[1], ''), ' ', NVL(split( `company name`, ' ' )[2], '')) as `company name`,
               location
        FROM CTE_prospects

        """)

        export.coalesce(1).write.mode("overwrite").option("header", True).csv(
            "/home/nan/gecko/code/companies-ingestion/prospect_ingestion/prospects/petshop_prospects/")

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
        # self.build_dados_empresa(df_dados_empresa=df_Dados_Empresas)
        self.log_process("[PASSO 3] - gravando dados de estabelecimentos !")
        # self.build_dados_estabelecimento(df_dados_estabelecimento=df_Dados_Estabelecimentos)
        self.log_process("[PASSO 4] - gravando dados de socios !")
        # self.build_dados_socios(df_dados_socios=df_Dados_Socios)
        self.log_process("[PASSO 5] - gravando dados intermediarios !")
        self.build_intermediario()
        self.log_process("[PASSO 6] - exportando dados !")
        self.export_data()
