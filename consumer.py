# consumer.py
import pika
import pandas as pd
import mysql.connector
import json
import os
from datetime import datetime

#Função para criar e retornar uma conexão com o MySQL
def get_mysql_connection():
    return mysql.connector.connect(
        host='localhost',
        user='root', #Informe o seu nome de usuário do MySQL aqui
        password='85854121', #Informe sua passowd do MySQL aqui
        database='db_intuitive_care'
    )

#Função para inserir uma tabela CSV convertida em DataFrame na tabela operadoras
def insert_operadoras_mysql(df):
    conn = get_mysql_connection() #Criação da conexção com o MySQL
    cursor = conn.cursor() #Criação de um cursor para executar os comandos SQL

    #Comando SQL que será usado para cada linha do CSV
    insert_query = """
        INSERT INTO operadoras (
            registro_ans, cnpj, razao_social, nome_fantasia, modalidade, logradouro,
            numero, complemento, bairro, cidade, uf, cep, ddd, telefone, fax,
            endereco_eletronico, representante, cargo_representante,
            regiao_de_comercializacao, data_registro_ans
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    #Laço que processa cada linha do DataFrame
    for index, row in df.iterrows(): #Seleciona cada linha como se fosse um dicionário
        #Checagem do campo registro_ans, não pode ser vazio ou NaN
        try:
            registro_ans = row.get('REGISTRO_ANS')
            if pd.isna(registro_ans) or str(registro_ans).strip().lower() == 'nan':
                print(f"[Linha {index}] Ignorada: REGISTRO_ANS inválido.")
                continue

            # Transformar DDD em string, mesmo que venha como float ou NaN
            ddd = row.get('DDD')
            try:
                ddd = str(int(float(ddd))) if pd.notna(ddd) else None
            except:
                ddd = None

            # Conversão de diferentes formatos de datas
            data_raw = row.get('DATA_REGISTRO_ANS')
            data_registro = None
            if pd.notna(data_raw):
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d']:
                    try:
                        data_registro = datetime.strptime(str(data_raw).strip(), fmt).date()
                        break
                    except:
                        continue
            #Checagem para o campo região
            regiao = row.get('REGIAO_DE_COMERCIALIZACAO')
            try:
                regiao = int(regiao) if pd.notna(regiao) else None
            except:
                regiao = None

            #Inserção de dados no banco, linha por linha
            cursor.execute(insert_query, (
                int(registro_ans),
                str(row.get('CNPJ') or '').strip() or None,
                str(row.get('RAZAO_SOCIAL') or '').strip() or None,
                str(row.get('NOME_FANTASIA') or '').strip() or None,
                str(row.get('MODALIDADE') or '').strip() or None,
                str(row.get('LOGRADOURO') or '').strip() or None,
                str(row.get('NUMERO') or '').strip() or None,
                str(row.get('COMPLEMENTO') or '').strip() or None,
                str(row.get('BAIRRO') or '').strip() or None,
                str(row.get('CIDADE') or '').strip() or None,
                str(row.get('UF') or '').strip() or None,
                str(row.get('CEP') or '').strip() or None,
                ddd,
                str(row.get('TELEFONE') or '').strip() or None,
                str(row.get('FAX') or '').strip() or None,
                str(row.get('ENDERECO_ELETRONICO') or '').strip() or None,
                str(row.get('REPRESENTANTE') or '').strip() or None,
                str(row.get('CARGO_REPRESENTANTE') or '').strip() or None,
                regiao,
                data_registro
            ))
        except Exception as e:
            print(f"[Linha {index}] Erro ao inserir: {e}")
            continue

    #Finalização da inserção
    conn.commit()
    cursor.close()
    conn.close()

#Função para inserir uma tabela CSV convertida em DataFrame na tabela demonstrativos_contabeis
def insert_demonstrativos_mysql(df):
    conn = get_mysql_connection()
    cursor = conn.cursor()

    #Normalização o DataFrame antes do loop, convertendo alguns campos
    df = df.copy()
    df['REG_ANS'] = pd.to_numeric(df['REG_ANS'], errors='coerce')
    df['VL_SALDO_INICIAL'] = df['VL_SALDO_INICIAL'].astype(str).str.replace(',', '.').astype(float)
    df['VL_SALDO_FINAL'] = df['VL_SALDO_FINAL'].astype(str).str.replace(',', '.').astype(float)
    df['DATA'] = pd.to_datetime(df['DATA'], errors='coerce').dt.date

    # Filtra apenas linhas com REG_ANS válidos na tabela operadoras (via SQL JOIN)
    valid_ids_query = "SELECT registro_ans FROM operadoras"
    cursor.execute(valid_ids_query)
    registros_validos = set(row[0] for row in cursor.fetchall())

    df = df[df['REG_ANS'].isin(registros_validos)]

    # Comando SQL que será usado para cada linha do CSV
    insert_query = """
        INSERT INTO demonstrativos_contabeis (
            data, reg_ans, cd_conta_contabil, descricao, vl_saldo_inicial, vl_saldo_final
        ) VALUES (%s, %s, %s, %s, %s, %s)
    """

    #Executar vários comandos de inserção em um único objeto, esssa tabela é mais pesada
    values = [
        (
            row['DATA'],
            int(row['REG_ANS']),
            row['CD_CONTA_CONTABIL'],
            row['DESCRICAO'],
            row['VL_SALDO_INICIAL'],
            row['VL_SALDO_FINAL']
        )
        for _, row in df.iterrows()
    ]

    try:
        cursor.executemany(insert_query, values) #executemany permite inserir várias linhas de uma vez
        conn.commit()
    except Exception as e:
        print(f"[ERRO] Insert em batch falhou: {e}")

    # Finalização da inserção
    cursor.close()
    conn.close()

#Função que consome a mensagem da fila, chamada sempre que chega nova mensagem do RabbitMQ
def callback(ch, method, properties, body):
    #Extrair dados da mensagem JSON(nome da tabela e caminho do CSV)
    msg = json.loads(body)
    tabela = msg['tabela']
    filePath_csv = msg['arquivo'].strip('"')

    #Se o arquivo não existe, dá erro e reconhece a mensagem como processada (não entra de novo).
    if not os.path.exists(filePath_csv):
        print(f"[ERRO] Arquivo não encontrado: {filePath_csv}")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        return

    #Lê o CSV como DataFrame e padroniza os nomes das colunas (tudo maiúsculo com underscore).
    print(f"[INFO] Importando para a tabela {tabela.upper()}: {filePath_csv}")
    df = pd.read_csv(filePath_csv, sep=';', encoding='utf-8')
    df.columns = [col.strip().upper().replace(' ', '_') for col in df.columns]

    #Decide qual função de importação executar com base na tabela enviada.
    if tabela == 'operadoras':
        insert_operadoras_mysql(df)
    elif tabela == 'demonstrativos_contabeis':
        insert_demonstrativos_mysql(df)

    print("[SUCESSO] Importação concluída!")
    ch.basic_ack(delivery_tag=method.delivery_tag)


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) #Cria uma conexão com o RabbitMQ
channel = connection.channel() #Cria uma ccanal de comunicação com o RabbitMQ
channel.queue_declare(queue='csv_import') #Cria a fila csv_import ou verifica se a mesma já existe
channel.basic_consume(queue='csv_import', on_message_callback=callback, auto_ack=False) #Chamar a função callback toda vez que tiver mensagem nova na fila
print('Mensagens estão sendo geradas..... \nSe quiser sair, pressione CTRL+C')
channel.start_consuming() #Inicio do consumo da fila
