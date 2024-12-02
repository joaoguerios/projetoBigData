import csv
import psycopg2

# Função para limpar e padronizar os campos de texto
def clean_name(name):
    # Converte para string, remove espaços no início e no fim, e substitui múltiplos espaços internos por um único espaço
    return ' '.join(str(name).strip().split())

# Conexão com o banco de dados
conn = psycopg2.connect(
    host="localhost",
    database="projeto2",
    user="postgres",
    password="#"
)
cur = conn.cursor()

# Conjuntos para armazenar países e plataformas únicos
countries = set()
platforms = set()
posts_data = []

# Leitura do arquivo CSV e coleta de dados
with open('sentimentdataset.csv', mode='r', encoding='utf-8') as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # Limpeza dos campos de texto
        for field in ['id', 'text', 'sentiment', 'user', 'platform', 'hashtags', 'country']:
            row[field] = clean_name(row[field])
        # Conversão dos campos numéricos
        for field in ['retweets', 'likes', 'year', 'month', 'day', 'hour']:
            try:
                row[field] = int(row[field])
            except (ValueError, TypeError):
                row[field] = None  # Define como None se não for possível converter

        countries.add(row['country'])
        platforms.add(row['platform'])
        
        posts_data.append(row)

# Inserção dos países com verificação de duplicatas
for country in countries:
    cur.execute("""
        INSERT INTO countries (country_name) 
        VALUES (%s) 
        ON CONFLICT (country_name) DO NOTHING;
    """, (country,))

# Inserção das plataformas com verificação de duplicatas
for platform in platforms:
    cur.execute("""
        INSERT INTO platforms (platform_name) 
        VALUES (%s) 
        ON CONFLICT (platform_name) DO NOTHING;
    """, (platform,))

conn.commit()

# Criação de dicionários para mapeamento
country_map = {}
cur.execute("SELECT country_id, country_name FROM countries;")
for row in cur.fetchall():
    country_map[row[1]] = row[0]

platform_map = {}
cur.execute("SELECT platform_id, platform_name FROM platforms;")
for row in cur.fetchall():
    platform_map[row[1]] = row[0]

# Inserção dos posts
for row in posts_data:
    country_id = country_map.get(row['country'])
    platform_id = platform_map.get(row['platform'])

    cur.execute("""
        INSERT INTO posts (
            id, text, sentiment, user_name, platform_id, hashtags, 
            retweets, likes, country_id, year, month, day, hour
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """, (
        row['id'], row['text'], row['sentiment'], row['user'], platform_id, row['hashtags'], 
        row['retweets'], row['likes'], country_id, row['year'], row['month'], 
        row['day'], row['hour']
    ))

conn.commit()
cur.close()
conn.close()