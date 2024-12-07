--1. Consulta Condicional Simples
SELECT 
    p.id, 
    p.text, 
    p.sentiment, 
    p.user, 
    p.hashtags, 
    p.retweets, 
    p.likes, 
    p.year, 
    p.month, 
    p.day, 
    p.hour, 
    c.country_name, 
    pl.platform_name
FROM posts p
JOIN countries c ON p.country_id = c.country_id
JOIN platforms pl ON p.platform_id = pl.platform_id
WHERE p.sentiment = "Positive";

--2. Busca de Texto
SELECT 
    p.id, 
    p.text, 
    p.sentiment, 
    p.user, 
    p.hashtags, 
    p.retweets, 
    p.likes, 
    p.year, 
    p.month, 
    p.day, 
    p.hour, 
    c.country_name, 
    pl.platform_name
FROM posts p
JOIN countries c ON p.country_id = c.country_id
JOIN platforms pl ON p.platform_id = pl.platform_id
WHERE CONTAINS(p.text, "workout", true);

--3. Cálculo da Média de Retweets por Plataforma
SELECT 
    p.platform_id, 
    COUNT(1) AS retweet_count, 
    SUM(p.retweets) AS total_retweets
FROM posts p
GROUP BY p.platform_id

--4. Média de Curtidas por País
SELECT 
    p.country_id, 
    COUNT(1) AS like_count, 
    SUM(p.likes) AS total_likes
FROM posts p
GROUP BY p.country_id