--1. Consulta Condicional Simples
SELECT p.id, p.text, p.sentiment, p.user, p.hashtags, p.retweets, p.likes, 
       p.year, p.month, p.day, p.hour, 
       p.country.name AS country_name, 
       p.platform.name AS platform_name
FROM Trabalho2 p
WHERE p.sentiment = 'Positive'

--2. Busca de Texto
SELECT p.id, p.text, p.sentiment, p.user, p.hashtags, p.retweets, p.likes, 
       p.year, p.month, p.day, p.hour, 
       p.country.name AS country_name, 
       p.platform.name AS platform_name
FROM Trabalho2 p
WHERE CONTAINS(p.text, 'workout', true)

--3. Cálculo da Média de Retweets por Plataforma
SELECT p.platform.id AS platform_id, p.platform.name AS platform_name, 
       COUNT(1) AS retweet_count, SUM(p.retweets) AS total_retweets
FROM Trabalho2 p
GROUP BY p.platform.id, p.platform.name

--4. Média de Curtidas por País
SELECT p.country.id AS country_id, p.country.name AS country_name, 
       COUNT(1) AS like_count, AVG(p.likes) AS average_likes
FROM Trabalho2 p
GROUP BY p.country.id, p.country.name
