CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Tabela: statuses
-- Status das reclamações
CREATE TABLE IF NOT EXISTS statuses (
    status_id SERIAL PRIMARY KEY,
    status VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_status_valid CHECK (status IN ('Open', 'Closed', 'In Progress', 'Assigned', 'Pending'))
);


-- Tabela: complaint_types
-- Tipos de reclamação
CREATE TABLE IF NOT EXISTS complaint_types (
    complaint_type_id SERIAL PRIMARY KEY,
    complaint_type VARCHAR(200) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabela: locations

CREATE TABLE IF NOT EXISTS locations (
    location_id SERIAL PRIMARY KEY,
    borough VARCHAR(50) NOT NULL DEFAULT 'Unspecified',
    city VARCHAR(100),
    incident_zip VARCHAR(10),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    x_coordinate_state_plane VARCHAR(20),
    y_coordinate_state_plane VARCHAR(20),
    location TEXT,
    location_type VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT chk_borough_valid CHECK (borough IN ('BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'Unspecified')),
    CONSTRAINT chk_latitude_valid CHECK (latitude IS NULL OR (latitude >= -90 AND latitude <= 90)),
    CONSTRAINT chk_longitude_valid CHECK (longitude IS NULL OR (longitude >= -180 AND longitude <= 180))
);

-- TABELA complaints

CREATE TABLE IF NOT EXISTS complaints (
    complaint_id SERIAL PRIMARY KEY,
    unique_key VARCHAR(50) UNIQUE NOT NULL,
    created_date TIMESTAMP NOT NULL,
    closed_date TIMESTAMP,
    status_id INTEGER NOT NULL,
    complaint_type_id INTEGER NOT NULL,
    location_id INTEGER NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Foreign Keys
    CONSTRAINT fk_complaints_status FOREIGN KEY (status_id) REFERENCES statuses(status_id),
    CONSTRAINT fk_complaints_complaint_type FOREIGN KEY (complaint_type_id) REFERENCES complaint_types(complaint_type_id),
    CONSTRAINT fk_complaints_location FOREIGN KEY (location_id) REFERENCES locations(location_id),
    -- Constraints de validação
    CONSTRAINT chk_closed_after_created CHECK (closed_date IS NULL OR closed_date >= created_date)
);

-- FUNÇÕES E TRIGGERS

-- Função para atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Trigger para complaints
DROP TRIGGER IF EXISTS update_complaints_updated_at ON complaints;
CREATE TRIGGER update_complaints_updated_at
    BEFORE UPDATE ON complaints
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Trigger para locations
DROP TRIGGER IF EXISTS update_locations_updated_at ON locations;
CREATE TRIGGER update_locations_updated_at
    BEFORE UPDATE ON locations
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DO $$
BEGIN
    RAISE NOTICE 'Tabelas criadas: statuses, complaint_types, locations, complaints';
END $$;
