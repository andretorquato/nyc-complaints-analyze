CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


CREATE SCHEMA IF NOT EXISTS public;
SET search_path TO public;

DO $$
BEGIN
    RAISE NOTICE 'Database nyc_complaints inicializado com sucesso!';
END $$;
