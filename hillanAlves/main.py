import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# carregando os arquivos enviados no SIGAA
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

# Lista de DataFrames para aplicar as operações
dfs_para_limpar = [leituras_setembro_df, leituras_outubro_df, moradores_df]

for df in dfs_para_limpar:
    # Limpa espaços em branco no início e fim (Obrigatório para merges)
    df['Lote'] = df['Lote'].str.strip()
    
    # Use replace com regex=False para hífens
    df['Lote'] = df['Lote'].str.replace('-', '', regex=False) 
    
    # Se houver outros caracteres, como espaços internos (ex: 'A 01'), remova-os:
    df['Lote'] = df['Lote'].str.replace(' ', '', regex=False)
    
    # Padroniza para Maiúsculas (Garante que 'a01' e 'A01' sejam iguais)
    df['Lote'] = df['Lote'].str.upper()

# Atalhos para os nomes das colunas
sete = 'Leitura_Setembro'
out = 'Leitura_Outubro'
bruto = 'Consumo_Bruto'

# Mistura os 2 df's de leitura de acordo com a coluna de coincidência deles (Lote)
leituras_merge_df = pd.merge(
    leituras_setembro_df, 
    leituras_outubro_df, 
    on='Lote',                 
    how='left',
    suffixes= ('_Setembro', '_Outubro')
)

# Criação da coluna de "Consumo Bruto" na df recém únida
leituras_merge_df[bruto] = leituras_merge_df[out] - leituras_merge_df[sete]

# List com os possíveis erros de leituras para facilitar a verificação
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

# Definindo Consumo Ajustado (Obrigatório: Consumo Ajustado = 0 para anomalias)
leituras_merge_df['Consumo Ajustado'] = np.where(
    leituras_merge_df['Status'].isin(status_consumo_zero),
    0,
    leituras_merge_df[bruto]
)

# Corringindo o Consumo_Bruto para Casos de 'Falta de Leitura'
leituras_merge_df[bruto] = np.where(
    leituras_merge_df['Status'] == 'Falta de Leitura',
    0,
    leituras_merge_df[bruto]
)

# Merge entre as leituras e os dados dos moradores
relatorio_merge_df = pd.merge(
    moradores_df, 
    leituras_merge_df, 
    on='Lote',                 
    how='left',  # Usamos 'left' para manter todos os moradores no relatório final
)

# --- GERAÇÃO DO RELATÓRIO FINAL ---

# Top 10 Maiores Consumidores (Status OK)
top_10_ok_df = relatorio_merge_df[relatorio_merge_df['Status'] == 'OK'].copy()
top_10_ok_df = top_10_ok_df.sort_values(
    by='Consumo Ajustado', 
    ascending=False
).head(10)

# Top 10 Anomalias (Todos os Status != OK)
anomalias_df = relatorio_merge_df[relatorio_merge_df['Status'] != 'OK'].copy()
top_10_anomalias_df = anomalias_df.sort_values(
    by=['Status', 'Consumo Ajustado'], 
    ascending=[True, False] # Ordena primeiro pelo Status para agrupar as categorias de erro
).head(10)

# --- MÉDIA E FAIXAS DE CONSUMO ---

consumo_valido_df = relatorio_merge_df[relatorio_merge_df['Status'] == 'OK'].copy()
media_consumo_ok = consumo_valido_df['Consumo Ajustado'].mean()

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

# DEFINIÇÃO DO LIMITE DE AGRUPAMENTO
LIMITE_AGRUPAMENTO = 3.0 # Agrupar faixas menores que 3% do total de lotes OK

# Aplicar o agrupamento 'Outros'
analise_faixas['Faixa_Agrupada'] = np.where(
    analise_faixas['% Lotes'] < LIMITE_AGRUPAMENTO,
    'Outros',
    analise_faixas['Faixa']
)

# Agrupar novamente pelos novos rótulos
analise_agrupada = analise_faixas.groupby('Faixa_Agrupada').agg(
    Contagem_Final=('Lotes_Contagem', 'sum'),
    Soma_Consumo_Final=('Soma_Consumo', 'sum')
).reset_index()

# Recalcular os Percentuais Finais
analise_agrupada['% Lotes Final'] = (analise_agrupada['Contagem_Final'] / total_lotes_ok) * 100
analise_agrupada['% Consumo Final'] = (analise_agrupada['Soma_Consumo_Final'] / total_consumo_ok) * 100

while 1:
    print('=' * 41)
    print('=== Menu - Analise de Consumo de Água ===')
    print('=' * 41)
    print('1 - Relatorio Final')
    print('2 - 10 maiores consumidores')
    print('3 - 10 maiores anomalias')
    print('4 - Distribuição de lotes por faixa de consumo (Gráfico)')
    print('5 - Porcentagem de consumo por faixa (Gráfico)')
    print('0 - Sair')

    resp = input().strip()

    match resp:
        case '0' | 'q' | 'sair':
            print('Saindo...')
            break

        case '1':
            # Relatório Final: mostra todos os moradores com suas leituras e status

            # Entrada robusta para a quantidade de linhas
            while True:
                max_rows = len(relatorio_merge_df)
                print(f'Insira a quantidade de linhas que deseja no relatório (Max: {max_rows}, ou "all" para todos): ')
                s = input().strip().lower()

                if s in ('all', 'a'):
                    n = max_rows
                    break

                try:
                    n = int(s)
                except ValueError:
                    print('Entrada inválida — digite um número inteiro ou "all".')
                    continue

                if n > max_rows:
                    print('O número de linhas não pode ser maior que o máximo.')
                elif n < 0:
                    print('O número não pode ser negativo!')
                else:
                    break

            try:
                cols = ['Lote', 'Nome', sete, out, bruto, 'Status', 'Consumo Ajustado', 'Email', 'Contato']

                # Verifica se todas as colunas existem antes de tentar exibir
                missing = [c for c in cols if c not in relatorio_merge_df.columns]
                if missing:
                    print(f'Atenção: algumas colunas não estão presentes no DataFrame: {missing}')
                    cols = [c for c in cols if c in relatorio_merge_df.columns]

                print(f"\n--- Relatório Final (mostrando {n} linhas) ---")
                print(relatorio_merge_df[cols].head(n).to_markdown(index=False))
            except Exception as e:
                print(f'Erro ao exibir Relatório Final: {e}')

        case '2':
            # Top 10 maiores consumidores (OK)
            try:
                if top_10_ok_df.empty:
                    print('Não há registros OK para exibir.')
                else:
                    print('\n--- Top 10 Maiores Consumidores (Status: OK) ---')
                    print(top_10_ok_df[['Lote', 'Nome', 'Consumo Ajustado', 'Email']].to_markdown(index=False))
            except Exception as e:
                print(f'Erro ao exibir Top 10 OK: {e}')

        case '3':
            # Top 10 anomalias
            try:
                if top_10_anomalias_df.empty:
                    print('Não há anomalias para exibir.')
                else:
                    print('\n--- Top 10 Maiores Anomalias ---')
                    print(top_10_anomalias_df[['Lote', 'Nome', bruto, 'Consumo Ajustado', 'Status', 'Contato']].to_markdown(index=False))
            except Exception as e:
                print(f'Erro ao exibir Top 10 Anomalias: {e}')

        case '4':
            # Distribuição de lotes por faixa de consumo (gráfico)
            try:
                if 'analise_agrupada' not in locals() or analise_agrupada.empty:
                    print('Análise de faixas não disponível. Refaça a análise antes de plotar.')
                else:
                    plt.figure(figsize=(10, 7))
                    plt.pie(
                        analise_agrupada['% Lotes Final'],
                        labels=analise_agrupada['Faixa_Agrupada'],
                        autopct='%1.1f%%',
                        startangle=90,
                        wedgeprops={'edgecolor': 'black'}
                    )
                    plt.title(f'Distribuição de Lotes por Faixa de Consumo (Limiar: < {LIMITE_AGRUPAMENTO}%)', fontsize=14)
                    plt.axis('equal')
                    plt.show()
            except Exception as e:
                print(f'Erro ao gerar gráfico de faixas: {e}')

        case '5':
            # Porcentagem de consumo por faixa (gráfico)
            try:
                if 'analise_agrupada' not in locals() or analise_agrupada.empty:
                    print('Análise de faixas não disponível. Refaça a análise antes de plotar.')
                else:
                    plt.figure(figsize=(10, 7))
                    plt.pie(
                        analise_agrupada['% Consumo Final'],
                        labels=analise_agrupada['Faixa_Agrupada'],
                        autopct='%1.1f%%',
                        startangle=90,
                        wedgeprops={'edgecolor': 'black'}
                    )
                    plt.title(f'Distribuição do Consumo Total por Faixa (Limiar: < {LIMITE_AGRUPAMENTO}%)', fontsize=14)
                    plt.axis('equal')
                    plt.show()
            except Exception as e:
                print(f'Erro ao gerar gráfico de consumo por faixa: {e}')

        case _:
            print('Opção inválida. Digite um número entre 0 e 5 e pressione Enter.')