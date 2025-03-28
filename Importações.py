import pandas as pd
import pymysql
import os

# 📌 Configuração da conexão com o banco de dados
db_config = {
    "host": "localhost",
    "user": "root",
    "password": "Yasmin14.",
    "database": "drivers_db",
    "port": 3306
}

# 📂 Caminho da pasta com os arquivos CSV
DATA_PATH = r"C:\Users\CR Express\Desktop\POWER BI CR MASTER\ProjetoFinanceiro"

try:
    # 📌 Conectar ao banco de dados
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    print("✅ Conectado ao banco de dados!")
except pymysql.MySQLError as err:
    print(f"❌ Erro ao conectar ao MySQL: {err}")
    exit()

# 📌 Processar todos os arquivos CSV da pasta
for file_name in os.listdir(DATA_PATH):
    if file_name.endswith(".csv"):
        file_path = os.path.join(DATA_PATH, file_name)
        print(f"📂 Processando arquivo: {file_name}")

        # 🗂️ Ler o arquivo CSV corretamente
        df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")
        print("🔍 Colunas encontradas:", df.columns.tolist())

        df.columns = df.columns.str.strip()  # Remove espaços dos nomes das colunas

        # 🗓️ Convertendo as colunas de data
        df['data_do_lancamento_financeiro'] = pd.to_datetime(df['data_do_lancamento_financeiro'], errors="coerce").dt.date
        df['data_do_periodo_de_referencia'] = pd.to_datetime(df['data_do_periodo_de_referencia'], errors="coerce").dt.date

        # 📌 Substituir valores nulos para evitar erros no MySQL
        df = df.fillna("")

        # 📌 Ajustar os valores da coluna 'valor' (substituindo vírgulas por ponto)
        df['valor'] = df['valor'].astype(str).str.replace(',', '.').astype(float)

        # 🚀 Inserindo dados no MySQL
        for _, row in df.iterrows():
            try:
                valores = (
                    row["id_da_pessoa_entregadora"], row["recebedor"],
                    row["data_do_lancamento_financeiro"], row["data_do_periodo_de_referencia"],
                    row["valor"], row["descricao"], row["tipo"],
                    row["praca"], row["subpraca"], row.get("outros_criterios", None)
                )

                cursor.execute("""
                    INSERT INTO financeiro (
                        id_da_pessoa_entregadora, recebedor, 
                        data_do_lancamento_financeiro, data_do_periodo_de_referencia, 
                        valor, descricao, tipo, praca, subpraca, outros_criterios
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE
                        valor = VALUES(valor), descricao = VALUES(descricao), tipo = VALUES(tipo),
                        praca = VALUES(praca), subpraca = VALUES(subpraca), outros_criterios = VALUES(outros_criterios);
                """, valores)

            except pymysql.MySQLError as err:
                print(f"⚠️ Erro ao inserir dados: {err}")
                continue

        print(f"✅ Importação de {file_name} concluída!")

        # 🗑️ Remover o arquivo após importação
        os.remove(file_path)
        print(f"🗑️ Arquivo {file_name} deletado após importação!")

# ✅ Confirmar transações
conn.commit()
cursor.close()
conn.close()
print("🎉 Todos os arquivos foram importados com sucesso!")
