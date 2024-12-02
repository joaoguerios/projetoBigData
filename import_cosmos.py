import pandas as pd
from azure.cosmos import exceptions, CosmosClient, PartitionKey

# Configuração do cliente Cosmos DB
endpoint = "#"
key = "#"

client = CosmosClient(endpoint, key)
database_name = 'BD3'
container_name = 'Trabalho2'

# Criação do banco de dados e container, se não existirem
database = client.create_database_if_not_exists(id=database_name)
container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path='/id'),
    offer_throughput=400
)

# Função para limpar e padronizar os campos de texto
def clean_text(text):
    # Remove espaços extras e converte para string, se necessário
    return ' '.join(str(text).strip().split())

# Leitura do arquivo CSV
df = pd.read_csv('sentimentdataset.csv', encoding='utf-8')

# Limpeza dos dados
df['id'] = df['id'].apply(clean_text)
df['text'] = df['text'].apply(clean_text)
df['sentiment'] = df['sentiment'].apply(clean_text)
df['user'] = df['user'].apply(clean_text)
df['platform'] = df['platform'].apply(clean_text)
df['hashtags'] = df['hashtags'].apply(clean_text)
df['retweets'] = pd.to_numeric(df['retweets'], errors='coerce')
df['likes'] = pd.to_numeric(df['likes'], errors='coerce')
df['country'] = df['country'].apply(clean_text)
df['year'] = pd.to_numeric(df['year'], errors='coerce').astype('Int64')
df['month'] = pd.to_numeric(df['month'], errors='coerce').astype('Int64')
df['day'] = pd.to_numeric(df['day'], errors='coerce').astype('Int64')
df['hour'] = pd.to_numeric(df['hour'], errors='coerce').astype('Int64')

# Remoção de linhas duplicadas com base no campo 'id'
df.drop_duplicates(subset=['id'], keep='last', inplace=True)

# Iteração e inserção dos itens no Cosmos DB
for index, row in df.iterrows():
    item = {
        "id": row["id"],
        "text": row["text"],
        "sentiment": row["sentiment"],
        "user": row["user"],
        "platform": row["platform"],
        "hashtags": row["hashtags"],
        "retweets": row["retweets"],
        "likes": row["likes"],
        "country": row["country"],
        "year": row["year"],
        "month": row["month"],
        "day": row["day"],
        "hour": row["hour"]
    }
    try:
        container.upsert_item(item)
    except exceptions.CosmosHttpResponseError as e:
        print(f'Erro ao inserir o item {item["id"]}: {e.message}')