# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import requests
from pathlib import Path
import pandas as pd

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

caminho_mco = "abfss://0009eafe-0886-46a1-b201-7dcfd0296dcb@onelake.dfs.fabric.microsoft.com/d38ddd19-3076-41b4-8e1b-c4a892045e38/Tables/dbo/transporte-bh03-mco-bronze"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mco = spark.read.format("delta").load(caminho_mco)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_mco = df_mco.limit(10)

# Converte para Pandas
df_mco = df_mco.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_mco)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
import requests
from datetime import datetime

# ---------------- CONFIGURAÇÕES ----------------
API_KEY = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJqdGkiOiJWTG9ZRTRTWGJJSDVHcnV4RkNudkF1V1g0eGJXVzAxV2tzUVhXR2RmTm5vIiwiaWF0IjoxNzYyOTEyNTY0fQ.bzrAoiZQJ4A4IPe9_wgvhMuLi5RasGJ662Zss3TL6BE"  # substitua pela sua chave de API do CKAN
CKAN_URL = "https://ckan.pbh.gov.br/api/3/action/resource_create"
PACKAGE_ID = "927871c6-6827-43fc-88d6-700cf8850a2a"  # ID do dataset no portal

# Nome automático com data/hora (ex: df_mco_2025_11_12_15h32.csv)
timestamp = datetime.now().strftime("%Y_%m_%d_%Hh%M")
nome_arquivo = f"MCO_{timestamp}.csv"
descricao = f"Arquivo gerado automaticamente via API em {timestamp}"

# ---------------- EXPORTAR O DATAFRAME ----------------
# Exemplo de DataFrame fictício (caso queira testar sem o df_mco real)
# df_mco = pd.DataFrame({"CHAVE_VIAGEM": [1, 2], "EXTENSAO": [12.5, 15.3]})

df_mco.to_csv(nome_arquivo, index=False, encoding="utf-8")

# ---------------- ENVIAR PARA O PORTAL ----------------
with open(nome_arquivo, "rb") as f:
    response = requests.post(
        CKAN_URL,
        headers={"Authorization": API_KEY},
        data={
            "package_id": PACKAGE_ID,
            "name": nome_arquivo,
            "description": descricao,
            "format": "CSV",
            "mimetype": "text/csv"
        },
        files=[("upload", f)],
    )

# ---------------- VERIFICAR RESULTADO ----------------
try:
    result = response.json()
except Exception:
    raise RuntimeError(f"❌ Erro na resposta da API: {response.text}")

if result.get("success"):
    resource = result["result"]
    print("✅ Upload realizado com sucesso!")
    print(f"Nome: {resource['name']}")
    print(f"ID do recurso: {resource['id']}")
    print(f"Link direto: {resource.get('url', 'Sem URL informada')}")
else:
    print("❌ Falha no upload:")
    print(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
