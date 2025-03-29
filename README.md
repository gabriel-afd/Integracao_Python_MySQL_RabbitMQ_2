
# Projeto de Importação Automatizada de Dados da ANS com RabbitMQ e MySQL

Este projeto tem como objetivo automatizar a importação de dados públicos da ANS (Agência Nacional de Saúde Suplementar) para um banco de dados MySQL. A comunicação entre os componentes é feita por meio do RabbitMQ. O sistema lê arquivos CSV e insere os dados nas tabelas `operadoras` e `demonstrativos_contabeis` de forma robusta e eficiente.

## Tecnologias Utilizadas

- Python 3.11+
- RabbitMQ
- MySQL
- Docker (opcional, para rodar o RabbitMQ)
- Bibliotecas: `pika`, `pandas`, `mysql-connector-python`

---

## Pré-Requisitos

### 1. MySQL

Você deve ter um banco de dados MySQL rodando com o banco `db_intuitive_care` criado. Dentro desse banco, execute os comandos abaixo para criar as tabelas:

```sql
CREATE TABLE operadoras(
    registro_ans INT PRIMARY KEY,
    cnpj VARCHAR(20),
    razao_social VARCHAR(255),
    nome_fantasia VARCHAR(255),
    modalidade VARCHAR(100),
    logradouro VARCHAR(255),
    numero VARCHAR(20),
    complemento VARCHAR(100),
    bairro VARCHAR(100),
    cidade VARCHAR(100),
    uf VARCHAR(2),
    cep VARCHAR(10),
    ddd VARCHAR(3),
    telefone VARCHAR(30),
    fax VARCHAR(30),
    endereco_eletronico VARCHAR(255),
    representante VARCHAR(255),
    cargo_representante VARCHAR(100),
    regiao_de_comercializacao INT,
    data_registro_ans DATE
);

CREATE TABLE demonstrativos_contabeis(
    id INT AUTO_INCREMENT PRIMARY KEY,
    data DATE,
    reg_ans INT,
    cd_conta_contabil VARCHAR(20),
    descricao VARCHAR(255),
    vl_saldo_inicial DECIMAL(18,2),
    vl_saldo_final DECIMAL(18,2),
    CONSTRAINT operadoras_demonstrativos_contabeis_fk FOREIGN KEY (reg_ans) 
        REFERENCES operadoras(registro_ans)
);
```

⚠️ No arquivo `consumer.py`, atualize os campos `user` e `password` com seu usuário e senha do MySQL:
```python
user='root',
password='SUA_SENHA',
```

---

### 2. RabbitMQ

Você pode instalar o RabbitMQ localmente ou rodá-lo em um container Docker:

#### 🐳 Rodando com Docker:
```bash
docker run -d --hostname my-rabbit --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3-management
```

> Acesse o painel do RabbitMQ em: http://localhost:15672  
> Login padrão: `guest`, senha: `guest`

---

## Instalação das Dependências

Execute o script abaixo em Python para instalar automaticamente as bibliotecas necessárias:

```python
import subprocess
import sys

required_packages = ['pika', 'pandas', 'mysql-connector-python']
for package in required_packages:
    try:
        __import__(package.split('-')[0])
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])
```

---

## Como Rodar o Projeto

### 1. Producer

Executa o script `producer.py` para enviar o caminho do CSV e a tabela desejada (operadoras ou demonstrativos_contabeis):

```bash
python producer.py
```

### 2. Consumer

Executa o script `consumer.py` para consumir a mensagem da fila e importar os dados:

```bash
python consumer.py
```

---

## Fontes dos Arquivos CSV

- **Operadoras Ativas**:  
  https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/

- **Demonstrativos Contábeis (últimos 2 anos)**:  
  https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/

---

## Observações

- A importação da tabela `demonstrativos_contabeis` só será bem-sucedida se os valores de `reg_ans` existirem na tabela `operadoras`.
- O sistema está preparado para ignorar valores inválidos ou registros incompletos.
- A inserção em batch garante performance otimizada para grandes volumes de dados.

---

## Autor

Desenvolvido por [Seu Nome].  
Em caso de dúvidas ou melhorias, sinta-se à vontade para contribuir!
