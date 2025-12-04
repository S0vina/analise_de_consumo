import pandas as pd

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
leituras_setembro_df['Lote'] = leituras_setembro_df['Lote'].str.strip()
leituras_outubro_df['Lote'] = leituras_outubro_df['Lote'].str.strip()
moradores_df['Lote'] = moradores_df['Lote'].str.strip()

# Mistura os 3 df's de acordo com a coluna de coincidência delas (Lote)
leituras_merge_df = pd.merge(
    leituras_setembro_df, 
    leituras_outubro_df, 
    on='Lote',                 
    how='inner',
    suffixes= ('_Setembro', '_Outubro')
)

# print(leituras_merge_df.head())
# print(f"\nNúmero total de lotes com ambas as leituras: {len(leituras_merge_df)}")

leituras_merge_df['Consumo_Bruto'] = leituras_merge_df['Leitura_Outubro'] - leituras_merge_df['Leitura_Setembro']

# print("/n -- Data frame do consumo bruto --")
print(leituras_merge_df.head())

sete = 'Leitura_Setembro'
out = 'Leitura_Outubro'
bruto = 'Consumo_Bruto'

# Função que analisa linha a linha, o status das medições do Lote
def analisa_status(linha) :

    val_set = linha[sete]
    val_out = linha[out]
    val_bruto = linha[bruto]

    if val_bruto < 0:
        return 'Consumo negativo/Invertido'
    
    elif val_bruto > 500:
        return 'Consumo excessivo'
    
    elif val_set < 0 or val_out < 0:
        return 'Leitura Invalida'
    
    else: 
        return 'OK'

# .apply altera linha a linha no df, valor retornado pela função
leituras_merge_df['Status'] = leituras_merge_df.apply(analisa_status, axis=1)

# print(leituras_merge_df)

# Adiciona 0 (não cobra) as leituras do mês de outubro as quais estão classificadas como erros
def corrigi_erros(linha):
    
    erros = ['Consumo negativo/Invertido', 'Leitura Invalida']

    if linha['Status'] in erros:
        return 0
    else:
        return linha[out]
    
# .apply aplica o valor retornado pela função em cada uma das linhas da coluna Leitura_Outubro
leituras_merge_df [out] = leituras_merge_df.apply(corrigi_erros, axis=1)

print(leituras_merge_df)


