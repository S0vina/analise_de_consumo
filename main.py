import pandas as pd

try:
    df1 = pd.read_csv("files/Moradores.csv")
    df2 = pd.read_csv("files/leitura_setembro.csv")
    df3 = pd.read_csv("files/leitura_outubro.csv")

except FileNotFoundError as e:
    print(f"Error {e}Ouve algum erro no carregamento dos documentos.")

moradores_df = pd.DataFrame(df1)
leituras_setembro_df = pd.DataFrame(df2)
leituras_outubro_df = pd.DataFrame(df3)

leituras_setembro_df['Lote'] = leituras_setembro_df['Lote'].str.strip()
leituras_outubro_df['Lote'] = leituras_outubro_df['Lote'].str.strip()
moradores_df['Lote'] = moradores_df['Lote'].str.strip()

leituras_merge_df = pd.merge(
    leituras_setembro_df, 
    leituras_outubro_df, 
    on='Lote',                 
    how='inner',
    suffixes= ('_Setembro', '_Outubro')
)

print(leituras_merge_df)

