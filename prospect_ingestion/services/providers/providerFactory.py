from services.providers.dados_gov import Dados_govProvider


class ProviderFactory:
    """
    Factory para construir a classe do provider em função do nome do mesmo.
    """

    @staticmethod
    def build(provider_name, spark, df_utils):
        # print ("global: ", globals())
        return globals()[f"{provider_name.capitalize()}Provider"](spark, df_utils)
