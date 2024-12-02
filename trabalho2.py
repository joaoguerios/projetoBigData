import pandas as pd
from azure.cosmos import CosmosClient

# Configurações do Cosmos DB
url = "https://cosmosrgeastusd10f3750-daf8-4546-afb0db.documents.azure.com:443/"
key = "K0TIhLyBJtGIjOqNH5O3JKFnOQZ5JlUIY4unCgxvLZrh2OIZ6vDj7LJp42vINNyxPvh23AMXSNiWACDbw2zvJg=="
database_name = "BD3"
container_name = "Trabalho2"

# Conexão com o Cosmos DB
client = CosmosClient(url, credential=key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Lendo o CSV gerado anteriormente
df = pd.read_csv("sentimentdataset.csv")

# Inserindo os dados no Cosmos DB
for _, row in df.iterrows():
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
    container.upsert_item(item)

