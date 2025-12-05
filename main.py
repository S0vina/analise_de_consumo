import pandas as pd
import matplotlib as mp
import numpy as np

try:
    df1 = pd.read_csv("files/Moradores.csv")
    df2 = pd.read_csv("files/leitura_setembro.csv")
    df3 = pd.read_csv("files/leitura_outubro.csv")

except FileNotFoundError as e:
    print(f"Error {e}Ouve algum erro no carregamento dos documentos.")

# Construtor dos df's
moradores_df = pd.DataFrame(df1)
leituras_setembro_df = pd.DataFrame(df2)
leituras_outubro_df = pd.DataFrame(df3)

# Tratamento das linhas vazias de cada uma das df's 
leituras_setembro_df['Lote'] = leituras_setembro_df['Lote'].str.strip('-')
leituras_outubro_df['Lote'] = leituras_outubro_df['Lote'].str.strip('-')
moradores_df['Lote'] = moradores_df['Lote'].str.strip('-')

# Mistura os 3 df's de acordo com a coluna de coincidência delas (Lote)
leituras_merge_df = pd.merge(
    leituras_setembro_df, 
    leituras_outubro_df, 
    on='Lote',                 
    how='left',
    suffixes= ('_Setembro', '_Outubro')
)

# print(leituras_merge_df.head())
# print(f"\nNúmero total de lotes com ambas as leituras: {len(leituras_merge_df)}")

leituras_merge_df['Consumo_Bruto'] = leituras_merge_df['Leitura_Outubro'] - leituras_merge_df['Leitura_Setembro']

# print("/n -- Data frame do consumo bruto --")
# print(leituras_merge_df.head())

sete = 'Leitura_Setembro'
out = 'Leitura_Outubro'
bruto = 'Consumo_Bruto'

status_consumo_zero = [
    'Falta de Leitura',
    'Consumo negativo/Invertido',
    'Leitura Invalida'
]

# Função que analisa linha a linha, o status das medições do Lote
def analisa_status(linha) :

    val_set = linha[sete]
    val_out = linha[out]
    val_bruto = linha[bruto]

    if pd.isna(val_set) or pd.isna(val_out) or pd.isna(val_bruto):
        return 'Falta de Leitura'
    
    if val_set < 0 or val_out < 0:
        return 'Leitura Invalida'

    elif val_bruto < 0:
        return 'Consumo negativo/Invertido'
    
    elif val_bruto > 500:
        return 'Consumo excessivo'
    
    else: 
        return 'OK'

# .apply altera linha a linha no df, valor retornado pela função
leituras_merge_df['Status'] = leituras_merge_df.apply(analisa_status, axis=1)

# --- INÍCIO DO BLOCO DE CORREÇÃO ESSENCIAL ---

# 1. Definir Consumo Ajustado (Obrigatório: Consumo Ajustado = 0 para anomalias)
# O Consumo Ajustado é a coluna que você usa para cobrar/analisar o consumo final.
leituras_merge_df['Consumo Ajustado'] = np.where(
    leituras_merge_df['Status'].isin(status_consumo_zero),
    0,
    leituras_merge_df[bruto]
)

# 2. Corrigir Consumo_Bruto para Casos de 'Falta de Leitura'
# O Consumo_Bruto deve ser corrigido para 0 APENAS para os casos de Falta de Leitura. 
# Isso garante que ele não seja NaN no relatório final.
leituras_merge_df[bruto] = np.where(
    leituras_merge_df['Status'] == 'Falta de Leitura',
    0,
    leituras_merge_df[bruto]
)

# 🚨 CORREÇÃO CRÍTICA DO MERGE: Deve ser 'left' para incluir lotes com Falta de Leitura.
relatorio_merge_df = pd.merge(
    moradores_df, 
    leituras_merge_df, 
    on='Lote',                 
    how='left',  # Usamos 'left' para manter todos os moradores no relatório final
)

print("\n--- ✅ DataFrame de Relatório Corrigido (Exemplo) ---")
print(relatorio_merge_df[['Lote', 'Nome', sete, out, bruto, 'Status', 'Consumo Ajustado']].head(10).to_markdown(index=False))

# --- GERAÇÃO DO RELATÓRIO FINAL ---

# 1. Top 10 Maiores Consumidores (Status OK)
# Deve ser feito no DataFrame completo, ordenando e pegando as 10 primeiras linhas.
top_10_ok_df = relatorio_merge_df[relatorio_merge_df['Status'] == 'OK'].copy()
top_10_ok_df = top_10_ok_df.sort_values(
    by='Consumo Ajustado', 
    ascending=False
).head(10)

print("\n--- 🏆 Top 10 Maiores Consumidores (Status: OK) ---")
print(top_10_ok_df[['Lote', 'Nome', 'Consumo Ajustado', 'Email']].to_markdown(index=False))


# 2. Top 10 Anomalias (Todos os Status != OK)
anomalias_df = relatorio_merge_df[relatorio_merge_df['Status'] != 'OK'].copy()
top_10_anomalias_df = anomalias_df.sort_values(
    by=['Status', 'Consumo Ajustado'], 
    ascending=[True, False] # Ordena primeiro pelo Status para agrupar as categorias de erro
).head(10)

print("\n--- ⚠️ Top 10 Maiores Anomalias (Falta, Negativo, Inválido, Excessivo) ---")
print(top_10_anomalias_df[['Lote', 'Nome', 'Consumo Ajustado', bruto, 'Status', 'Contato']].to_markdown(index=False))


# --- ANÁLISE ADICIONAL: MÉDIA E FAIXAS DE CONSUMO ---

consumo_valido_df = relatorio_merge_df[relatorio_merge_df['Status'] == 'OK'].copy()
media_consumo_ok = consumo_valido_df['Consumo Ajustado'].mean()

print(f"\n--- 📊 Média de Consumo (Casos OK) ---")
print(f"A média do Consumo Ajustado para lotes OK é: {media_consumo_ok:.2f} m³")

# Faixas de consumo
bins = [0, 50, 100, 150, 200, 300, consumo_valido_df['Consumo Ajustado'].max() + 1]
labels = ['0 a 50', '51 a 100', '101 a 150', '151 a 200', '201 a 300', '> 300']

consumo_valido_df['Faixa'] = pd.cut(
    consumo_valido_df['Consumo Ajustado'], 
    bins=bins, 
    labels=labels, 
    right=True,
    include_lowest=True
)

analise_faixas = consumo_valido_df.groupby('Faixa').agg(
    Lotes_Contagem=('Lote', 'count'),
    Soma_Consumo=('Consumo Ajustado', 'sum')
).reset_index()

total_lotes_ok = analise_faixas['Lotes_Contagem'].sum()
total_consumo_ok = analise_faixas['Soma_Consumo'].sum()

analise_faixas['% Lotes'] = (analise_faixas['Lotes_Contagem'] / total_lotes_ok) * 100
analise_faixas['% Consumo'] = (analise_faixas['Soma_Consumo'] / total_consumo_ok) * 100

print("\n--- 📈 Análise de Consumo por Faixa ---")
print(analise_faixas.to_markdown(index=False, floatfmt=".2f"))