import re
import uuid
import time
import json
import glob
import fnmatch
import unidecode
import boto3
from email_validator import validate_email, EmailNotValidError
from pyspark.sql.functions import *
from pyspark.sql.types import *


class DataCleansing:
    """
        Abstracao de classes para tratamento e manipulacao de dados
    """

    def __init__(self, spark):
        self.spark = spark

    @staticmethod
    @udf(returnType=StringType())
    def is_valid_cpf(cpf):
        """ Validates a brazilian CPF """
        # Check if type is str
        if cpf is not None and len(cpf) > 0:
            if not isinstance(cpf, str):
                return ""
            # Remove some unwanted characters
            cpf = re.sub("[^0-9]", '', cpf)
            # Verify if CPF number is equal
            if cpf == '00000000000' or cpf == '11111111111' or cpf == '22222222222' or cpf == '33333333333' or cpf == '44444444444' or cpf == '55555555555' or cpf == '66666666666' or cpf == '77777777777' or cpf == '88888888888' or cpf == '99999999999':
                return ""
            # Checks if string has 11 characters
            if len(cpf) != 11:
                return ""
            sum = 0
            weight = 10
            """ Calculating the first cpf check digit. """
            for n in range(9):
                sum = sum + int(cpf[n]) * weight
                # Decrement weight
                weight = weight - 1
            verifyingDigit = 11 - sum % 11
            if verifyingDigit > 9:
                firstVerifyingDigit = 0
            else:
                firstVerifyingDigit = verifyingDigit
            """ Calculating the second check digit of cpf. """
            sum = 0
            weight = 11
            for n in range(10):
                sum = sum + int(cpf[n]) * weight
                # Decrement weight
                weight = weight - 1
            verifyingDigit = 11 - sum % 11
            if verifyingDigit > 9:
                secondVerifyingDigit = 0
            else:
                secondVerifyingDigit = verifyingDigit
            if cpf[-2:] == "%s%s" % (firstVerifyingDigit, secondVerifyingDigit):
                return cpf
            return False

    @staticmethod
    @udf(returnType=StringType())
    def check_phone_type(phone_number):
        try:
            phone_size = len(phone_number)
            if (phone_size == 11) and (phone_number[2] == "9"):
                return "mobile"
            if phone_size == 10:
                return "fixo"
            return "0"
        except:
            pass

    @staticmethod
    @udf(returnType=StringType())
    def check_email(email):
        if email is not None and len(email) > 0:
            pat = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
            if re.match(pat, email):
                return email
            else:
                ''

    @staticmethod
    @udf(returnType=StringType())
    def remove_accents(string):
        string = string if string is not None else ''
        return unidecode.unidecode(string)

    @staticmethod
    @udf(returnType=StringType())
    def check_uf_state(uf_state=None):

        states = [
            dict(
                check_names=['acre', 'ácre'],
                name="Acre",
                short_name="AC"
            ),
            dict(
                check_names=['alagoas', 'álagoas'],
                name="Alagoas",
                short_name="AL"
            ),
            dict(
                check_names=['amapá', 'amapa'],
                name="Amapá",
                short_name="AM"
            ),
            dict(
                check_names=['amazonas', 'amazon', 'ámazonia', 'amazonia'],
                name="Amazonas",
                short_name="AM"
            ),
            dict(
                check_names=['bahia', 'báhia', 'baia'],
                name="Bahia",
                short_name="BA"
            ),
            dict(
                check_names=['ceará', 'ceara', 'ceára'],
                name="Ceará",
                short_name="CE"
            ),
            dict(
                check_names=['distrito federal', 'df'],
                name="Distrito Federal",
                short_name="DF"
            ),
            dict(
                check_names=['espirito santo', 'espírito santo', 'espirito santos'],
                name="Espírito Santo",
                short_name="ES"
            ),
            dict(
                check_names=['goias', 'goiás', 'góias', 'goías'],
                name="Goiás",
                short_name="GO"
            ),
            dict(
                check_names=['maranhao', 'maranhão'],
                name="Maranhão",
                short_name="MA"
            ),
            dict(
                check_names=['mato grosso', 'máto grosso'],
                name="Mato Grosso",
                short_name="MT"
            ),
            dict(
                check_names=['mato grosso do sul', 'máto grosso do sul'],
                name="Mato Grosso do Sul",
                short_name="MS"
            ),
            dict(
                check_names=['minas gerais', 'minas'],
                name="Minas Gerais",
                short_name="MG"
            ),
            dict(
                check_names=['para', 'pará'],
                name="Pará",
                short_name="PA"
            ),
            dict(
                check_names=['paraiba', 'paraíba'],
                name="Paraíba",
                short_name="PB"
            ),
            dict(
                check_names=['parana', 'paraná'],
                name="Paraná",
                short_name="PR"
            ),
            dict(
                check_names=['pernambuco', 'pernambúco'],
                name="Pernanbuco",
                short_name="PE"
            ),
            dict(
                check_names=['piaui', 'piauí', 'píaui'],
                name="Piauí",
                short_name="PI"
            ),
            dict(
                check_names=['rio grande do norte', 'rio grande norte'],
                name="Rio Grande do Norte",
                short_name="RN"
            ),
            dict(
                check_names=['rio grande do sul', 'rio grande sul'],
                name="Rio Grande do Sul",
                short_name="RS"
            ),
            dict(
                check_names=['rondonia', 'rôndonia', 'rondônia'],
                name="Rondônia",
                short_name="RO"
            ),
            dict(
                check_names=['roraima', 'roraíma', 'roráima'],
                name="Roraima",
                short_name="RR"
            ),
            dict(
                check_names=['santa catarina', 'santa cátarina'],
                name="Santa Catarina",
                short_name="SC"
            ),
            dict(
                check_names=['sao paulo', 'sp', 'sampa', 'são paulo', 'são-paulo', 'sao-paulo'],
                name="São Paulo",
                short_name="SP"
            ),
            dict(
                check_names=['sergipe', 'serjipe', 'sergípe'],
                name="Sergipe",
                short_name="SE"
            ),
            dict(
                check_names=['tocantins', 'tocantíns'],
                name="Tocantins",
                short_name="TO"
            ),
            dict(
                check_names=['rio', 'rio de janeiro', 'rio janeiro', 'riio'],
                name="Rio de Janeiro",
                short_name="RJ"
            )
        ]

        # print("# VALIDATING UF ")
        # print(uf_state)

        if uf_state is not None and len(uf_state) == 2:
            if uf_state.upper() in [i['short_name'] for i in states]:
                return uf_state.upper()

        if uf_state is not None and len(uf_state) > 2:
            if uf_state.upper() in [i['name'] for i in states]:
                return uf_state.upper()

        if len(uf_state) == 0:
            return uf_state
        else:
            return f"uf invalida - {uf_state}"

    @staticmethod
    @udf(returnType=StringType())
    def remove_ddis(phone_number):
        try:
            if len(phone_number) > 0 and phone_number[:2] == 55:
                return phone_number[2:]
            else:
                return phone_number
        except:
            pass

    @staticmethod
    @udf(returnType=StringType())
    def check_ddds(ddd):
        ddd_states = [
            dict(
                ddd="11",
                state="SP"
            ),
            dict(
                ddd="12",
                state="SP"
            ),
            dict(
                ddd="13",
                state="SP"
            ),
            dict(
                ddd="14",
                state="SP"
            ),
            dict(
                ddd="15",
                state="SP"
            ),
            dict(
                ddd="16",
                state="SP"
            ),
            dict(
                ddd="17",
                state="SP"
            ),
            dict(
                ddd="18",
                state="SP"
            ),
            dict(
                ddd="19",
                state="SP"
            ),
            dict(
                ddd="21",
                state="RJ"
            ),
            dict(
                ddd="22",
                state="RJ"
            ),
            dict(
                ddd="24",
                state="RJ"
            ),
            dict(
                ddd="27",
                state="ES"
            ),
            dict(
                ddd="28",
                state="ES"
            ),
            dict(
                ddd="31",
                state="MG"
            ),
            dict(
                ddd="32",
                state="MG"
            ),
            dict(
                ddd="33",
                state="MG"
            ),
            dict(
                ddd="34",
                state="MG"
            ),
            dict(
                ddd="35",
                state="MG"
            ),
            dict(
                ddd="37",
                state="MG"
            ),
            dict(
                ddd="38",
                state="MG"
            ),
            dict(
                ddd="41",
                state="PR"
            ),
            dict(
                ddd="42",
                state="PR"
            ),
            dict(
                ddd="43",
                state="PR"
            ),
            dict(
                ddd="44",
                state="PR"
            ),
            dict(
                ddd="45",
                state="PR"
            ),
            dict(
                ddd="46",
                state="PR"
            ),
            dict(
                ddd="47",
                state="SC"
            ),
            dict(
                ddd="48",
                state="SC"
            ),
            dict(
                ddd="49",
                state="SC"
            ),
            dict(
                ddd="51",
                state="RS"
            ),
            dict(
                ddd="53",
                state="RS"
            ),
            dict(
                ddd="54",
                state="RS"
            ),
            dict(
                ddd="55",
                state="RS"
            ),
            dict(
                ddd="61",
                state="DF"
            ),
            dict(
                ddd="62",
                state="GO"
            ),
            dict(
                ddd="63",
                state="TO"
            ),
            dict(
                ddd="64",
                state="GO"
            ),
            dict(
                ddd="65",
                state="MT"
            ),
            dict(
                ddd="66",
                state="MT"
            ),
            dict(
                ddd="67",
                state="MS"
            ),
            dict(
                ddd="68",
                state="AC"
            ),
            dict(
                ddd="69",
                state="RO"
            ),
            dict(
                ddd="71",
                state="BA"
            ),
            dict(
                ddd="73",
                state="BA"
            ),
            dict(
                ddd="74",
                state="BA"
            ),
            dict(
                ddd="75",
                state="BA"
            ),
            dict(
                ddd="77",
                state="BA"
            ),
            dict(
                ddd="79",
                state="SE"
            ),
            dict(
                ddd="81",
                state="PE"
            ),
            dict(
                ddd="82",
                state="AL"
            ),
            dict(
                ddd="83",
                state="PB"
            ),
            dict(
                ddd="84",
                state="RN"
            ),
            dict(
                ddd="85",
                state="CE"
            ),
            dict(
                ddd="86",
                state="PI"
            ),
            dict(
                ddd="87",
                state="PE"
            ),
            dict(
                ddd="88",
                state="CE"
            ),
            dict(
                ddd="89",
                state="PI"
            ),
            dict(
                ddd="91",
                state="PA"
            ),
            dict(
                ddd="92",
                state="AM"
            ),
            dict(
                ddd="93",
                state="PA"
            ),
            dict(
                ddd="94",
                state="PA"
            ),
            dict(
                ddd="95",
                state="RR"
            ),
            dict(
                ddd="96",
                state="AP"
            ),
            dict(
                ddd="97",
                state="AM"
            ),
            dict(
                ddd="98",
                state="MA"
            ),
            dict(
                ddd="99",
                state="MA"
            )
        ]

        if ddd is not None and len(ddd) == 2:
            if ddd in [i['ddd'] for i in ddd_states]:
                return ddd

        return f"DDD invalido - {ddd}"

    @staticmethod
    @udf(returnType=StringType())
    def transf_address(address, logradouro=False):
        address_types = [
            dict(
                short_address_type="AV",
                address_type="AVENIDA"
            ),
            dict(
                short_address_type="AL",
                address_type="ALAMEDA"
            ),
            dict(
                short_address_type="BC",
                address_type="BECO"
            ),
            dict(
                short_address_type="CJ",
                address_type="CONJUNTO"
            ),
            dict(
                short_address_type="EST",
                address_type="ESTRADA"
            ),
            dict(
                short_address_type="LOT",
                address_type="LOTE"
            ),
            dict(
                short_address_type=['PR', 'PC'],
                address_type="PRACA"
            ),
            dict(
                short_address_type="Q",
                address_type="QUADRA"
            ),
            dict(
                short_address_type="R",
                address_type="RUA"
            ),
            dict(
                short_address_type="ROD",
                address_type="RODOVIA"
            ),
            dict(
                short_address_type="TV",
                address_type="TRAVESSA"
            )
        ]

        address = '' if address is None else address
        result = ''

        if len(address) > 0:
            if not logradouro:
                result = [address_types[i].get('address_type')
                          for i in range(0, len([i for i in address_types]))
                          if address.upper() in address_types[i].get('short_address_type')
                          ]
                return result[0] if len(result) > 0 else address.upper()

            else:
                if address.split(' ')[0].upper() in [address_types[i].get('address_type') for i in
                                                     range(0, len([i for i in address_types]))]:
                    junkie = address.split(' ')[0]
                    result = address.replace(junkie, '')
                    return result

                else:
                    return address.upper()
        else:
            return address.upper()

    def main(self):

        cpf_valido = DataCleansing.is_valid_cpf
        check_phone_type_v = DataCleansing.check_phone_type
        get_email = DataCleansing.check_email
        purge_accents = DataCleansing.remove_accents
        check_state = DataCleansing.check_uf_state
        check_ddd = DataCleansing.check_ddds
        remove_ddi = DataCleansing.remove_ddis
        t_address = DataCleansing.transf_address

        self.spark.udf.register("udf_cpf_valido", cpf_valido)

        self.spark.udf.register("udf_check_phone_type", check_phone_type_v)

        self.spark.udf.register("udf_check_email", get_email)

        self.spark.udf.register("udf_remove_accents", purge_accents)

        self.spark.udf.register("udf_check_state", check_state)

        self.spark.udf.register("udf_check_ddd", check_ddd)

        self.spark.udf.register("udf_remove_ddi", remove_ddi)

        self.spark.udf.register("udf_transf_address", t_address)

    if __name__ == "__main__":
        main()
