-- CONSULTA 1: Filtros por Data e Intervalos - Reclamações por Período
SELECT 
    DATE(created_date) as data,
    s.status,
    COUNT(*) as total_reclamacoes
FROM complaints c
JOIN statuses s ON c.status_id = s.status_id
WHERE c.created_date BETWEEN '2025-01-01' AND '2026-01-15'
GROUP BY DATE(created_date), s.status
ORDER BY data, s.status;

-- CONSULTA 2: Agregação - Top N Problemas por Bairro
WITH ranked_complaints AS (
    SELECT 
        l.borough,
        ct.complaint_type,
        COUNT(*) as count,
        ROUND((COUNT(*)::numeric / SUM(COUNT(*)) OVER (PARTITION BY l.borough)) * 100, 2) as proporcao,
        RANK() OVER (PARTITION BY l.borough ORDER BY COUNT(*) DESC) as rank
    FROM complaints c
    JOIN locations l ON c.location_id = l.location_id
    JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
    WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    GROUP BY l.borough, ct.complaint_type
)
SELECT 
    borough,
    complaint_type,
    count,
    proporcao,
    rank
FROM ranked_complaints
WHERE rank <= 10
ORDER BY borough, rank;


-- CONSULTA 3: Junções - Matriz de Similaridade entre Bairros
WITH proportions_matrix AS (
    SELECT 
        l.borough,
        ct.complaint_type,
        (COUNT(*)::numeric / SUM(COUNT(*)) OVER (PARTITION BY l.borough)) * 100 as proporcao
    FROM complaints c
    JOIN locations l ON c.location_id = l.location_id
    JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
    WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    GROUP BY l.borough, ct.complaint_type
)
SELECT 
    p1.borough as borough1,
    p2.borough as borough2,
    ROUND(
        SUM(p1.proporcao * p2.proporcao) / 
        (SQRT(SUM(POWER(p1.proporcao, 2))) * SQRT(SUM(POWER(p2.proporcao, 2)))), 
        4
    ) as correlacao_pearson
FROM proportions_matrix p1
JOIN proportions_matrix p2 ON p1.complaint_type = p2.complaint_type AND p1.borough < p2.borough
GROUP BY p1.borough, p2.borough
ORDER BY correlacao_pearson DESC;


-- PARTE 2
-- CONSULTA: Tempo Médio de Resolução (dias)
SELECT 
    AVG(EXTRACT(EPOCH FROM (closed_date - created_date))/86400) as dias_medio_resolucao
FROM complaints
WHERE closed_date IS NOT NULL;


-- CONSULTA: Taxa de Fechamento (%)
SELECT 
    COUNT(*) FILTER (WHERE closed_date IS NOT NULL) * 100.0 / COUNT(*) as taxa_fechamento_percentual
FROM complaints;

-- Analisa reclamações por período, status e tipo com múltiplos intervalos temporais
SELECT 
    DATE_TRUNC('month', c.created_date) as mes,
    DATE_TRUNC('week', c.created_date) as semana,
    s.status,
    ct.complaint_type,
    COUNT(*) as total_reclamacoes,
    COUNT(*) FILTER (WHERE c.closed_date IS NOT NULL) as reclamacoes_fechadas,
    AVG(EXTRACT(EPOCH FROM (c.closed_date - c.created_date))/86400) FILTER (WHERE c.closed_date IS NOT NULL) as tempo_medio_resolucao_dias,
    MIN(c.created_date) as primeira_reclamacao,
    MAX(c.created_date) as ultima_reclamacao
FROM complaints c
JOIN statuses s ON c.status_id = s.status_id
JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
WHERE c.created_date >= CURRENT_DATE - INTERVAL '6 months'
    AND c.created_date < CURRENT_DATE
    AND s.status IN ('Open', 'Closed', 'In Progress')
GROUP BY 
    DATE_TRUNC('month', c.created_date),
    DATE_TRUNC('week', c.created_date),
    s.status,
    ct.complaint_type
HAVING COUNT(*) >= 5
ORDER BY mes DESC, semana DESC, total_reclamacoes DESC;

-- Análise estatística detalhada de reclamações por bairro e tipo
WITH estatisticas_por_bairro_tipo AS (
    SELECT 
        l.borough,
        ct.complaint_type,
        COUNT(*) AS total,
        COUNT(DISTINCT c.location_id) AS locais_unicos,
        COUNT(DISTINCT DATE(c.created_date)) AS dias_com_reclamacoes,
        COUNT(*) FILTER (WHERE c.closed_date IS NOT NULL) AS fechadas,
        COUNT(*) FILTER (WHERE c.closed_date IS NULL) AS abertas,

        ROUND(
            AVG(
                EXTRACT(EPOCH FROM (c.closed_date - c.created_date)) / 86400
            ) FILTER (WHERE c.closed_date IS NOT NULL),
            2
        ) AS tempo_medio_dias,

        ROUND(
            (
                PERCENTILE_CONT(0.5)
                WITHIN GROUP (
                    ORDER BY EXTRACT(EPOCH FROM (c.closed_date - c.created_date)) / 86400
                )
                FILTER (WHERE c.closed_date IS NOT NULL)
            )::numeric,
            2
        ) AS tempo_mediano_dias,

        MIN(c.created_date) AS primeira_ocorrencia,
        MAX(c.created_date) AS ultima_ocorrencia
    FROM complaints c
    JOIN locations l ON c.location_id = l.location_id
    JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
    WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    GROUP BY l.borough, ct.complaint_type
),
rankings AS (
    SELECT 
        borough,
        complaint_type,
        total,
        locais_unicos,
        dias_com_reclamacoes,
        fechadas,
        abertas,
        ROUND((fechadas::numeric / NULLIF(total, 0)) * 100, 2) AS taxa_fechamento_pct,
        tempo_medio_dias,
        tempo_mediano_dias,
        primeira_ocorrencia,
        ultima_ocorrencia,
        RANK() OVER (PARTITION BY borough ORDER BY total DESC) AS rank_total,
        RANK() OVER (PARTITION BY borough ORDER BY tempo_medio_dias DESC NULLS LAST) AS rank_tempo_resolucao
    FROM estatisticas_por_bairro_tipo
    WHERE total >= 10
)
SELECT 
    borough,
    complaint_type,
    total,
    locais_unicos,
    dias_com_reclamacoes,
    fechadas,
    abertas,
    taxa_fechamento_pct,
    tempo_medio_dias,
    tempo_mediano_dias,
    primeira_ocorrencia,
    ultima_ocorrencia,
    rank_total,
    rank_tempo_resolucao,
    CASE 
        WHEN rank_total <= 3 THEN 'Alta Frequência'
        WHEN rank_total <= 10 THEN 'Média Frequência'
        ELSE 'Baixa Frequência'
    END AS categoria_frequencia
FROM rankings
ORDER BY borough, rank_total;

SELECT 
    l.borough,
    l.city,
    s.status,
    ct.complaint_type,
    COUNT(*) as total_reclamacoes,
    COUNT(DISTINCT c.complaint_id) as reclamacoes_unicas,
    COUNT(DISTINCT DATE(c.created_date)) as dias_ativos,
    
    ROUND(AVG(EXTRACT(EPOCH FROM (c.closed_date - c.created_date))/86400) FILTER (WHERE c.closed_date IS NOT NULL), 2) as tempo_medio_resolucao,
    ROUND(MIN(EXTRACT(EPOCH FROM (c.closed_date - c.created_date))/86400) FILTER (WHERE c.closed_date IS NOT NULL), 2) as tempo_minimo_resolucao,
    ROUND(MAX(EXTRACT(EPOCH FROM (c.closed_date - c.created_date))/86400) FILTER (WHERE c.closed_date IS NOT NULL), 2) as tempo_maximo_resolucao,
    
    DATE_TRUNC('month', MIN(c.created_date)) as mes_primeira_reclamacao,
    DATE_TRUNC('month', MAX(c.created_date)) as mes_ultima_reclamacao,
    
    ROUND((COUNT(*) FILTER (WHERE c.closed_date IS NOT NULL)::numeric / COUNT(*)) * 100, 2) as taxa_fechamento_pct,
    ROUND((COUNT(*) FILTER (WHERE s.status = 'Open')::numeric / COUNT(*)) * 100, 2) as taxa_abertas_pct,
    ROUND((COUNT(*) FILTER (WHERE s.status = 'In Progress')::numeric / COUNT(*)) * 100, 2) as taxa_em_andamento_pct
FROM complaints c
JOIN statuses s ON c.status_id = s.status_id
JOIN locations l ON c.location_id = l.location_id
JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    AND c.created_date >= CURRENT_DATE - INTERVAL '12 months'
GROUP BY 
    l.borough,
    l.city,
    s.status,
    ct.complaint_type
HAVING COUNT(*) >= 3
ORDER BY 
    l.borough,
    total_reclamacoes DESC,
    s.status,
    ct.complaint_type;