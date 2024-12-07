--1. Consulta Condicional Simples
SELECT p.*, c.country_name, pl.platform_name
FROM posts p
JOIN countries c ON p.country_id = c.country_id
JOIN platforms pl ON p.platform_id = pl.platform_id
WHERE p.sentiment = 'Positive';

--2. Busca de Texto
SELECT p.*, c.country_name, pl.platform_name
FROM posts p
JOIN countries c ON p.country_id = c.country_id
JOIN platforms pl ON p.platform_id = pl.platform_id
WHERE p.text ILIKE '%workout%';

--3. Cálculo da Média de Retweets por Plataforma
SELECT pl.platform_name, AVG(p.retweets) AS avg_retweets
FROM posts p
JOIN platforms pl ON p.platform_id = pl.platform_id
GROUP BY pl.platform_name
ORDER BY avg_retweets DESC;

--4. Média de Curtidas por País
SELECT c.country_name, AVG(p.likes) AS avg_likes
FROM posts p
JOIN countries c ON p.country_id = c.country_id
GROUP BY c.country_name
ORDER BY avg_likes DESC;