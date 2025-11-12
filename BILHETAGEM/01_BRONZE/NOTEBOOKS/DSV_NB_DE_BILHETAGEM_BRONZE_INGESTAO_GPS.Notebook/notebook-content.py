# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": "",
# META       "known_lakehouses": [
# META         {
# META           "id": "51b55ba5-c4df-4551-a4c1-3c7e001f616e"
# META         }
# META       ]
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

import sys
import subprocess

try:
    import requests_toolbelt
except ImportError:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "requests_toolbelt"])
    import requests_toolbelt

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# IMPORTAR BIBLIOTECAS

# CELL ********************

import csv
import json
import os
import re
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
import pytz

# PySpark imports
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, FloatType, IntegerType, StringType, TimestampType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

caminho_gps = "abfss://0d200be7-7ebd-417e-b638-cc9a8e06bd2b@onelake.dfs.fabric.microsoft.com/51b55ba5-c4df-4551-a4c1-3c7e001f616e/Tables/bilhetagem-tacom-gps_convencional-bronze"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# DEFINIR DATA/HORA DA IMPORTAÇÃO

# CELL ********************

retroagir_dias = 2
data = (datetime.now() - timedelta(days=retroagir_dias)).strftime("%d/%m/%Y")
hora_inicial = "00:00:00"
hora_final = "23:59:59"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# AUTENTICAÇÃO

# CELL ********************

username = 'daniel.mnogueira'
password = 'Bh.160509'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def registrar_log(data_info, status, erro_msg=""):
    log = Row(
        nome_processo="gps_onibus_api",
        data_informacao=datetime.strptime(data_info, "%d/%m/%Y").strftime("%Y-%m-%d"),
        data_processamento=datetime.now(pytz.timezone("America/Sao_Paulo")),
        status=status,
        erro=erro_msg
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def obter_token():
    try:
        url = 'https://citgisnext.sitbus.com.br:9998/ecitbus-security-service-bhz/oauth/token'
        headers = {
            'Authorization': 'Basic ZWNpdGJ1cy1zZXJ2aWNlOiNzZW5oYVNlcnZpY2VGaW5AbDIwMThmaW5hbCQ=',
            'Content-Type': 'multipart/form-data'
        }
        data = MultipartEncoder(fields={
            'username': username,
            'password': password,
            'grant_type': 'password'
        })

        headers['Content-Type'] = data.content_type
        response = requests.post(url, headers=headers, data=data)

        if response.status_code == 200:
            return response.json().get('access_token')
        else:
            erro_msg = f"Erro ao obter token: {response.status_code} - {response.text}"
            registrar_log(data, "ERRO", erro_msg)
            return None
    except Exception as e:
        registrar_log(data, "ERRO", f"Exceção ao obter token: {str(e)}")
        return None

# 🔹 Função para exportar eventos de localização
def exportar_eventos(token, data, hora_inicial, hora_final):
    try:
        url = 'http://srvcitgis.sitbus.com.br:9971/gisexporterdataservice/viagens/exportarEventosLocalizacoes'
        headers = {
            'Authorization': f'Bearer {token}',
            'Content-Type': 'application/json'
        }
        payload = {
            "data": data,
            "horaInicial": hora_inicial,
            "horaFinal": hora_final
        }
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            return response.text
        else:
            erro_msg = f"Erro ao exportar eventos: {response.status_code} - {response.text}"
            registrar_log(data, "ERRO", erro_msg)
            return None
    except Exception as e:
        registrar_log(data, "ERRO", f"Exceção ao exportar eventos: {str(e)}")
        return None

# 🔹 Função para formatar txt para csv
def formatar_txt_para_csv(txt_file_path, csv_file_path):
    try:
        with open(txt_file_path, 'r') as f:
            conteudo = f.read()
        conteudo_com_quebra = conteudo.replace('>', '>\n')
        registros = conteudo_com_quebra.splitlines()
        dados_formatados = [registro.replace('<', '').replace('>', '').split(';') for registro in registros]

        with open(csv_file_path, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=';')
            csvwriter.writerow(['EV', 'LG', 'LT', 'NV', 'HR'])
            for linha in dados_formatados:
                csvwriter.writerow([campo.split('=')[0] if '=' in campo else '' for campo in linha])
    except Exception as e:
        registrar_log(data, "ERRO", f"Exceção ao formatar arquivo TXT: {str(e)}")

# 🔹 Função principal
def gps_por_horas():
    try:
        token = obter_token()
        if token:
            all_dataframes = []
            hora_inicio_dt = datetime.strptime(hora_inicial, "%H:%M:%S")
            hora_fim_dt = datetime.strptime(hora_final, "%H:%M:%S")

            while hora_inicio_dt <= hora_fim_dt:
                hora_final_intervalo = min(hora_inicio_dt + timedelta(hours=1), hora_fim_dt)
                hora_inicio_str = hora_inicio_dt.strftime("%H:%M:%S")
                hora_final_str = hora_final_intervalo.strftime("%H:%M:%S")

                eventos = exportar_eventos(token, data, hora_inicio_str, hora_final_str)

                if eventos:
                    pattern = r"<EV=(\d+);LG=([-]?\d+\.\d+);LT=([-]?\d+\.\d+);NV=(\d+);HR=(\d+)>"
                    matches = re.findall(pattern, eventos)

                    if matches:
                        df_eventos = pd.DataFrame(matches, columns=['EV', 'LG', 'LT', 'NV', 'HR'])
                        df_eventos = df_eventos.astype({'EV': 'int', 'LG': 'float', 'LT': 'float', 'NV': 'int', 'HR': 'int'})
                        all_dataframes.append(df_eventos)
                hora_inicio_dt += timedelta(hours=1)

            if all_dataframes:
                df_resultado = pd.concat(all_dataframes, ignore_index=True)
                registrar_log(data, "SUCESSO")
                return df_resultado
            else:
                registrar_log(data, "AVISO", "Nenhum dado foi coletado para o período informado.")
                return None
        else:
            registrar_log(data, "ERRO", "Token não foi obtido.")
            return None
    except Exception as e:
        registrar_log(data, "ERRO", f"Exceção geral na coleta de GPS: {str(e)}")
        return None

# 🔹 Execução
df_eventos = gps_por_horas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eventos = df_eventos.rename(columns={
    'EV': 'evento',
    'LG': 'longitude',
    'LT': 'latitude',
    'NV': 'veiculo',
    'HR': 'data_hora'
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fuso_brasil = pytz.timezone('America/Sao_Paulo')

# Data/hora atual no fuso brasileiro
df_eventos['data_criacao'] = pd.Timestamp.now(tz=fuso_brasil).strftime('%Y-%m-%d %H:%M:%S')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eventos['data_hora'] = df_eventos['data_hora'].astype(str)

df_eventos['ano'] = df_eventos['data_hora'].str[0:4].astype(int)
df_eventos['mes'] = df_eventos['data_hora'].str[4:6].astype(int)
df_eventos['dia'] = df_eventos['data_hora'].str[6:8].astype(int)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_eventos_spark = spark.createDataFrame(df_eventos)

df_eventos_spark = (
    df_eventos_spark
    .withColumn("evento", F.col("evento").cast("int"))
    .withColumn("longitude", F.col("longitude").cast("float"))
    .withColumn("latitude", F.col("latitude").cast("float"))
    .withColumn("veiculo", F.col("veiculo").cast("int"))
    .withColumn("data_hora", F.col("data_hora").cast("string"))
    .withColumn("data_criacao", F.col("data_criacao").cast("string"))
    .withColumn("ano", F.col("ano").cast("int"))
    .withColumn("mes", F.col("mes").cast("int"))
    .withColumn("dia", F.col("dia").cast("int"))
)

df_eventos_spark \
    .write \
    .format("delta") \
    .mode("append") \
    .partitionBy("ano", "mes", "dia") \
    .save(caminho_gps)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# GRAVAR LOG

# CELL ********************

df_gps = df_eventos_spark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gps = df_gps.withColumn(
    "data_informacao",
    F.to_date(F.col("data_hora").substr(1, 8), "yyyyMMdd")
)

# 2️⃣ Adicionar a coluna 'status' com valor 'sucesso'
df_gps = df_gps.withColumn("status", F.lit("sucesso"))

# 3️⃣ Remover a coluna original 'data_hora'
df_gps = df_gps.select("data_informacao", "data_criacao", "status")

# 4️⃣ Remover duplicados mantendo apenas um registro por data_informacao + data_criacao
df_gps = df_gps.dropDuplicates(["data_informacao", "data_criacao"])

# 5️⃣ (Opcional) Ordenar o resultado para visualização
df_gps = df_gps.orderBy("data_informacao", "data_criacao", "status")

df_gps = df_gps.withColumn("tabela", F.lit("api_gps"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

caminho_log = "abfss://0d200be7-7ebd-417e-b638-cc9a8e06bd2b@onelake.dfs.fabric.microsoft.com/51b55ba5-c4df-4551-a4c1-3c7e001f616e/Tables/bilhetagem-tacom-log_api-bronze"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gps \
    .write \
    .format("delta") \
    .mode("append") \
    .save(caminho_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_gps \
    .write \
    .format("delta") \
    .mode("append") \
    .save(caminho_log)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Tabela Salva")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
