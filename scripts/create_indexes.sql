-- ÍNDICES PARA QUERY 1: Filtros por Data e Status
-- Índice em created_date para range scans
CREATE INDEX IF NOT EXISTS idx_complaints_created_date 
ON complaints(created_date);

-- Índice em status_id para JOIN com statuses
CREATE INDEX IF NOT EXISTS idx_complaints_status_id 
ON complaints(status_id);

-- Índice composto funcional para otimizar GROUP BY e JOIN simultaneamente
CREATE INDEX IF NOT EXISTS idx_complaints_date_status_func 
ON complaints(DATE(created_date), status_id);

-- ÍNDICES PARA QUERIES 2 E 5: Agregações por Borough e Tipo

-- Índice em location_id para JOIN com locations (Queries 2 e 5)
CREATE INDEX IF NOT EXISTS idx_complaints_location_id 
ON complaints(location_id);

-- Índice em complaint_type_id para JOIN com complaint_types (Queries 2 e 5)
CREATE INDEX IF NOT EXISTS idx_complaints_complaint_type_id 
ON complaints(complaint_type_id);

-- Índice em borough (tabela locations) - CRÍTICO para Queries 2 e 5
CREATE INDEX IF NOT EXISTS idx_locations_borough 
ON locations(borough);

-- Índice composto (complaint_type_id, location_id) - OTIMIZA Queries 2 e 5
CREATE INDEX IF NOT EXISTS idx_complaints_complaint_type_location_id 
ON complaints(complaint_type_id, location_id);

-- Atualizar estatísticas do planner após criação dos índices
ANALYZE complaints;
ANALYZE locations;
ANALYZE complaint_types;
ANALYZE statuses;

DO $$
BEGIN
    RAISE NOTICE 'Índices: idx_complaints_created_date, idx_complaints_status_id, idx_complaints_date_status_func';
    RAISE NOTICE 'Índices: idx_complaints_location_id, idx_complaints_complaint_type_id, idx_locations_borough, idx_complaints_complaint_type_location_id';
END $$;
