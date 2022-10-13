
# Desafio de Orquestração do Airflow - Indicium



O Desafio foi composto pela execução de 4 tarefas:

- Criação de uma task que leia a tabela "Order", a qual exporta a consulta para um arquivo "output_orders.csv"
- Criação de uma task que leia a tabela "OrderDetail" e cruze os dados através de uma consulta com o arquivo csv criado anteriormente, para contabilizar a quantidade de pedidos com destino ao Rio de Janeiro e armazenar em um arquivo .txt
- Adição de variável no ambiente do Airflow
- Criação da ordenação de tarefas do Airflow

# Ferramentas/Database Utilizadas

- requirements.txt
- Airflow
- Venv
- Banco de dados: Northwhind_small.sqlite

# Utilização

## O Airflow

Site e documentação do Airflow:

    https://airflow.apache.org/


## Download do repositório

    https://github.com/kevinalvez/Desafio_Airflow.git

## Instlação de dependências

    Utilize o arquivo requirements.txt para verificar e instalar dependências necessárias

## Observações

- Verificar o install.sh para instalação do Airflow
- Alterar caminhos relativos da DAG caso necessário para execução
- Banco de dados em formato sqlite3

## Melhorias

- Remover códigos py para fora da dag e realizar apenas a chamada de execução
- Realizar alterações para execução através do Docker
