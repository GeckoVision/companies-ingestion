import unidecode
import pandas as pd
from pandas.testing import assert_frame_equal
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
import uuid
import time
import json
import glob
import fnmatch
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from utils.df_utils import DataframeService
import configparser
from services.providers.providerFactory import ProviderFactory


class DataIngestion(DataframeService):
    """
        Abstracao de classes para ingestao de dados
    """

    def __init__(self, spark):
        super().__init__(spark)

    def do_process(self, provider_name):
        self.log_process(f"Iniciando processamento do provider - {provider_name}")
        provider = ProviderFactory.build(provider_name, self.spark, DataframeService)
        provider.run()

    def run(self):
        super().run()
        provider_list = ["dados_gov"]
        [self.do_process(provider_name) for provider_name in provider_list]
