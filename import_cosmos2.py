import pandas as pd
from azure.cosmos import exceptions, CosmosClient, PartitionKey

# Configuração do cliente Cosmos DB
endpoint = "#"
key = "#"

client = CosmosClient(endpoint, key)
database_name = 'BD3'
container_name = 'Trabalho2'
country_container_name = 'Country'
platform_container_name = 'Platform'

# Criação do banco de dados e containers, se não existirem
database = client.create_database_if_not_exists(id=database_name)

main_container = database.create_container_if_not_exists(
    id=container_name,
    partition_key=PartitionKey(path='/id'),
    offer_throughput=400
)

country_container = database.create_container_if_not_exists(
    id=country_container_name,
    partition_key=PartitionKey(path='/country_id'),
    offer_throughput=400
)   

platform_container = database.create_container_if_not_exists(
    id=platform_container_name,
    partition_key=PartitionKey(path='/platform_id'),
    offer_throughput=400
)

# Função para limpar e padronizar os campos de texto
def clean_text(text):
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

# Normalização de países e plataformas
unique_countries = df['country'].unique()
unique_platforms = df['platform'].unique()

# Inserção das entidades normalizadas em coleções separadas
country_map = {}
for country_id, country in enumerate(unique_countries, start=1):
    country_map[country] = str(country_id)
    try:
        country_container.upsert_item({"country_id": str(country_id), "country_name": country})
    except exceptions.CosmosHttpResponseError as e:
        print(f'Erro ao inserir país {country}: {e.message}')

platform_map = {}
for platform_id, platform in enumerate(unique_platforms, start=1):
    platform_map[platform] = str(platform_id)
    try:
        platform_container.upsert_item({"platform_id": str(platform_id), "platform_name": platform})
    except exceptions.CosmosHttpResponseError as e:
        print(f'Erro ao inserir plataforma {platform}: {e.message}')

# Atualização do DataFrame com IDs normalizados
df['country_id'] = df['country'].map(country_map)
df['platform_id'] = df['platform'].map(platform_map)

# Iteração e inserção dos itens no container principal
for index, row in df.iterrows():
    item = {
        "id": row["id"],
        "text": row["text"],
        "sentiment": row["sentiment"],
        "user": row["user"],
        "platform_id": row["platform_id"],
        "hashtags": row["hashtags"],
        "retweets": row["retweets"],
        "likes": row["likes"],
        "country_id": row["country_id"],
        "year": row["year"],
        "month": row["month"],
        "day": row["day"],
        "hour": row["hour"]
    }
    try:
        main_container.upsert_item(item)
    except exceptions.CosmosHttpResponseError as e:
        print(f'Erro ao inserir o item {item["id"]}: {e.message}')
