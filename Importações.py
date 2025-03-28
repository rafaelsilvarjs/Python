import pandas as pd
import pymysql
import os
from dotenv import load_dotenv

# 🔹 Carregar variáveis de ambiente do .env
load_dotenv()

# 📌 Configuração da conexão com o banco de dados usando variáveis de ambiente
db_config = {
    "host": os.getenv("DB_HOST"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
    "port": int(os.getenv("DB_PORT", 3306))
}

# 📂 Caminho da pasta com os arquivos CSV
DATA_PATH = os.getenv("DATA_PATH", "./data")

try:
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

        df = pd.read_csv(file_path, delimiter=";", encoding="utf-8")
        df.columns = df.columns.str.strip()

        df['data_do_lancamento_financeiro'] = pd.to_datetime(df['data_do_lancamento_financeiro'], errors="coerce").dt.date
        df['data_do_periodo_de_referencia'] = pd.to_datetime(df['data_do_periodo_de_referencia'], errors="coerce").dt.date
        df['valor'] = df['valor'].astype(str).str.replace(',', '.').astype(float)

        df = df.fillna("")

        for _, row in df.iterrows():
            valores = (
                row["id_da_pessoa_entregadora"], row["recebedor"],
                row["data_do_lancamento_financeiro"], row["data_do_periodo_de_referencia"],
                row["valor"], row["descricao"], row["tipo"],
                row["praca"], row["subpraca"], row.get("outros_criterios", None)
            )

            try:
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
        os.remove(file_path)
        print(f"🗑️ Arquivo {file_name} deletado após importação!")

conn.commit()
cursor.close()
conn.close()
print("🎉 Todos os arquivos foram importados com sucesso!")
