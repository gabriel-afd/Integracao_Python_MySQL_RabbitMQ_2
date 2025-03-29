# producer.py
import pika
import json

# Coleta dados do usuário
tabela = input("Para qual tabela deseja importar os dados (operadoras ou demonstrativos_contabeis)? ").strip()
csv_path = input("Informe o caminho do arquivo CSV(copie o caminho do CSV): ").strip()

#Dicionário que é transformado em mensagem JSON e enviado ao RabbitMQ
message = {
    "tabela": tabela,
    "arquivo": csv_path
}

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost')) #Abre uma conexão com RabbitMQ
channel = connection.channel() #Cria um canal de comunicação dentro da conexão com o RAbbitMQ
channel.queue_declare(queue='csv_import') #Garante que a fila csv_import exista

#Aqui a mensagem é enviada para a fila csv_import
channel.basic_publish(
    exchange='', #usa o exchange padrão do RabbitMQ
    routing_key='csv_import', #Nome da fila para qual a mensagem será enviada
    body=json.dumps(message) #Converte o dicionário message para string JSON
)
print(f"Mensagem enviada com a tabela '{tabela}' e o caminho: {csv_path}")
connection.close()