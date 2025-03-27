import json
import pandas as pd

caminho_arquivo = 'jam_mock.json'

with open(caminho_arquivo, 'r') as arquivo:
    dados = json.load(arquivo)

df = pd.DataFrame(dados)

df = df.rename(columns={
    'country':'tx_pais',
    'level': 'li_nivel',
    'city':'tx_cidade',
    'speedKMH':'db_velocidade_kmh',
    'length': 'li_comprimento',
    'uuid':'tx_uuid',
    'endNode': 'tx_final',
    'speed': 'db_velocidade',
    'roadType': 'tx_tipo_via',
    'delay': 'li_atraso',
    'street': 'tx_rua',
    'id': 'li_id',
})
df.drop(columns=['line', 'turnType', 'segments'], axis=1, inplace=True)

df.to_excel('planilha.xlsx', index=False, engine='openpyxl')