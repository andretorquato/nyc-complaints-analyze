
-- CONSULTA 1
SET enable_seqscan = ON;

EXPLAIN ANALYZE
SELECT 
    DATE(created_date) as data,
    s.status,
    COUNT(*) as total_reclamacoes
FROM complaints c
JOIN statuses s ON c.status_id = s.status_id
WHERE c.created_date BETWEEN '2025-01-01' AND '2026-01-15'
GROUP BY DATE(created_date), s.status
ORDER BY data, s.status;


-- CONSULTA 2

SET enable_seqscan = ON;

EXPLAIN ANALYZE
WITH totals_borough AS (
    SELECT 
        l.borough,
        COUNT(*) as total_borough
    FROM complaints c
    JOIN locations l ON c.location_id = l.location_id
    WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    GROUP BY l.borough
),
counts_by_type_borough AS (
    SELECT 
        l.borough,
        ct.complaint_type,
        COUNT(*) as count
    FROM complaints c
    JOIN locations l ON c.location_id = l.location_id
    JOIN complaint_types ct ON c.complaint_type_id = ct.complaint_type_id
    WHERE l.borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND')
    GROUP BY l.borough, ct.complaint_type
)
SELECT 
    cbb.borough,
    cbb.complaint_type,
    cbb.count,
    tb.total_borough,
    ROUND((cbb.count::numeric / tb.total_borough::numeric) * 100, 2) as proporcao_percentual
FROM counts_by_type_borough cbb
JOIN totals_borough tb ON cbb.borough = tb.borough
ORDER BY cbb.borough, proporcao_percentual DESC;


-- CONSULTA 3

SET enable_seqscan = ON;

EXPLAIN ANALYZE
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
