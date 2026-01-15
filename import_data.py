import csv
import sys
import os
import argparse
import psycopg2
from psycopg2.extras import execute_batch
from datetime import datetime
from collections import defaultdict

# Valores NULL comuns
NULL_VALUES = ['', 'NULL', 'null', 'N/A', 'n/a', 'Unspecified', 'unspecified', 'NA', 'na']
VALID_BOROUGHS = {'BRONX', 'BROOKLYN', 'MANHATTAN', 'QUEENS', 'STATEN ISLAND', 'Unspecified'}


def normalize_value(value):
    """Normaliza valores, retornando None se vazio ou NULL"""
    if value is None:
        return None
    value = str(value).strip()
    if value == '' or value in NULL_VALUES:
        return None
    return value


def normalize_borough(value):
    """Normaliza borough para valores válidos"""
    normalized = normalize_value(value)
    if normalized is None:
        return 'Unspecified'
    normalized = normalized.upper().strip()
    if normalized == 'STATEN ISLAND' or normalized.startswith('STATEN'):
        return 'STATEN ISLAND'
    if normalized not in VALID_BOROUGHS:
        return 'Unspecified'
    return normalized


def normalize_status(value):
    """Normaliza status para valores válidos conforme constraint chk_status_valid"""
    # Valores válidos: 'Open', 'Closed', 'In Progress', 'Assigned', 'Pending'
    normalized = normalize_value(value)
    if normalized is None:
        return 'Open'
    
    normalized = normalized.strip()
    
    # Mapear valores inválidos para válidos
    status_mapping = {
        'Started': 'In Progress',
        'started': 'In Progress',
        'STARTED': 'In Progress',
    }
    
    if normalized in status_mapping:
        return status_mapping[normalized]
    
    # Se já é um valor válido, retornar como está
    valid_statuses = {'Open', 'Closed', 'In Progress', 'Assigned', 'Pending'}
    if normalized in valid_statuses:
        return normalized
    
    # Para qualquer outro valor inválido, usar 'Open' como padrão
    return 'Open'


def validate_coordinates(lat, lon):
    """Valida coordenadas"""
    try:
        if lat:
            lat_f = float(lat)
            if lat_f < -90 or lat_f > 90:
                return False, None, None
        if lon:
            lon_f = float(lon)
            if lon_f < -180 or lon_f > 180:
                return False, None, None
        return True, float(lat) if lat else None, float(lon) if lon else None
    except (ValueError, TypeError):
        return False, None, None


def validate_date(date_str):
    """Valida e converte data"""
    if not date_str:
        return None
    try:
        if 'T' in date_str:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        return datetime.fromisoformat(date_str)
    except (ValueError, AttributeError):
        return None


def validate_closed_date(created_date, closed_date):
    """Valida que closed_date não seja anterior a created_date (conforme constraint chk_closed_after_created)"""
    if closed_date is None or created_date is None:
        return closed_date
    
    # Se closed_date for anterior a created_date, retornar None para evitar violar constraint
    if closed_date < created_date:
        return None
    
    return closed_date


def get_db_connection(host, port, database, user, password):
    """Conecta ao banco de dados PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        conn.autocommit = False
        return conn
    except psycopg2.Error as e:
        print(f"ERRO ao conectar ao banco de dados: {e}")
        sys.exit(1)


def get_or_create_id(conn, table, column, value, insert_sql=None, insert_params=None, id_column=None):
    """Obtém ou cria ID em tabela de referência, fazendo commit imediato para garantir persistência"""
    if value is None:
        return None
    
    # Determinar o nome da coluna ID
    if id_column is None:
        id_column = f"{table}_id"
    
    cur = conn.cursor()
    
    # Tentar buscar existente
    cur.execute(f"SELECT {id_column} FROM {table} WHERE {column} = %s", (value,))
    result = cur.fetchone()
    
    if result:
        return result[0]
    
    # Criar novo se não existir e fazer commit imediato
    if insert_sql:
        cur.execute(insert_sql, insert_params or (value,))
        new_id = cur.fetchone()[0]
    else:
        cur.execute(f"INSERT INTO {table} ({column}) VALUES (%s) RETURNING {id_column}", (value,))
        new_id = cur.fetchone()[0]
    
    # Commit imediato para garantir que o registro persista mesmo se o batch de complaints falhar
    conn.commit()
    return new_id


def get_or_create_location_id(conn, borough, city, zip_code, lat, lon, x_coord, y_coord, location, location_type):
    """Obtém ou cria location_id, fazendo commit imediato para garantir persistência"""
    borough = normalize_borough(borough) or 'Unspecified'
    
    cur = conn.cursor()
    
    # Tentar encontrar location existente
    cur.execute("""
        SELECT location_id FROM locations 
        WHERE borough = %s 
          AND (latitude = %s OR (latitude IS NULL AND %s IS NULL))
          AND (longitude = %s OR (longitude IS NULL AND %s IS NULL))
        LIMIT 1
    """, (borough, lat, lat, lon, lon))
    
    result = cur.fetchone()
    if result:
        return result[0]
    
    # Criar nova location e fazer commit imediato
    cur.execute("""
        INSERT INTO locations (borough, city, incident_zip, latitude, longitude, 
                              x_coordinate_state_plane, y_coordinate_state_plane, 
                              location, location_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING location_id
    """, (borough, city, zip_code, lat, lon, x_coord, y_coord, location, location_type))
    
    new_id = cur.fetchone()[0]
    
    # Commit imediato para garantir que o registro persista mesmo se o batch de complaints falhar
    conn.commit()
    return new_id


def import_csv(conn, csv_file, batch_size=100):
    """Importa dados do CSV para o banco"""
    stats = {
        'total': 0,
        'success': 0,
        'errors': 0,
        'errors_details': []
    }
    
    print(f"Importando dados de: {csv_file}")
    print("=" * 80)
    
    # Cache para IDs de referência
    status_cache = {}
    complaint_type_cache = {}
    
    with open(csv_file, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        
        batch = []
        
        for row_num, row in enumerate(reader, start=2):
            stats['total'] += 1
            
            try:
                # Normalizar dados essenciais
                unique_key = normalize_value(row.get('unique_key'))
                created_date = validate_date(row.get('created_date'))
                closed_date_raw = validate_date(row.get('closed_date'))
                status = normalize_status(row.get('status'))
                complaint_type = normalize_value(row.get('complaint_type'))
                
                # Validar dados críticos
                if not unique_key or not created_date or not complaint_type:
                    stats['errors'] += 1
                    stats['errors_details'].append(f"Linha {row_num}: unique_key, created_date ou complaint_type inválido")
                    continue
                
                # Validar que closed_date não seja anterior a created_date (constraint chk_closed_after_created)
                closed_date = validate_closed_date(created_date, closed_date_raw)
                
                # Validar coordenadas
                lat = normalize_value(row.get('latitude'))
                lon = normalize_value(row.get('longitude'))
                valid_coords, lat_float, lon_float = validate_coordinates(lat, lon)
                
                # Obter/criar IDs de referência
                if status not in status_cache:
                    status_id = get_or_create_id(conn, 'statuses', 'status', status, id_column='status_id')
                    status_cache[status] = status_id
                else:
                    status_id = status_cache[status]
                
                if complaint_type not in complaint_type_cache:
                    complaint_type_id = get_or_create_id(conn, 'complaint_types', 'complaint_type', complaint_type, id_column='complaint_type_id')
                    complaint_type_cache[complaint_type] = complaint_type_id
                else:
                    complaint_type_id = complaint_type_cache[complaint_type]
                
                # Obter/criar location_id
                location_id = get_or_create_location_id(
                    conn,
                    row.get('borough'),
                    normalize_value(row.get('city')),
                    normalize_value(row.get('incident_zip')),
                    lat_float,
                    lon_float,
                    normalize_value(row.get('x_coordinate_state_plane')),
                    normalize_value(row.get('y_coordinate_state_plane')),
                    normalize_value(row.get('location')),
                    normalize_value(row.get('location_type'))
                )
                
                # Preparar inserção de complaint
                batch.append((
                    unique_key,
                    created_date,
                    closed_date,
                    status_id,
                    complaint_type_id,
                    location_id
                ))
                
                # Processar batch
                if len(batch) >= batch_size:
                    try:
                        cur = conn.cursor()
                        execute_batch(
                            cur,
                            """INSERT INTO complaints (unique_key, created_date, closed_date, 
                                                       status_id, complaint_type_id, location_id)
                               VALUES (%s, %s, %s, %s, %s, %s)""",
                            batch
                        )
                        conn.commit()
                        stats['success'] += len(batch)
                        batch = []
                    except psycopg2.Error as e:
                        # Erro SQL no batch: fazer rollback e limpar cache para forçar re-busca
                        conn.rollback()
                        # Limpar caches para evitar usar IDs que podem ter sido revertidos
                        status_cache.clear()
                        complaint_type_cache.clear()
                        stats['errors'] += len(batch)
                        stats['errors_details'].append(f"Linha {row_num - len(batch) + 1} a {row_num}: Erro no batch - {str(e)}")
                        batch = []
                    
                    # Progress indicator
                    if stats['total'] % 1000 == 0:
                        print(f"  Processadas {stats['total']:,} linhas... (Sucesso: {stats['success']:,}, Erros: {stats['errors']:,})")
            
            except psycopg2.Error as e:
                # Erro SQL: fazer rollback e limpar cache para permitir que a transação continue
                conn.rollback()
                # Limpar caches para evitar usar IDs que podem ter sido revertidos
                status_cache.clear()
                complaint_type_cache.clear()
                stats['errors'] += 1
                stats['errors_details'].append(f"Linha {row_num}: {str(e)}")
                continue
            except Exception as e:
                stats['errors'] += 1
                stats['errors_details'].append(f"Linha {row_num}: {str(e)}")
                continue
        
        # Processar batch final
        if batch:
            try:
                cur = conn.cursor()
                execute_batch(
                    cur,
                    """INSERT INTO complaints (unique_key, created_date, closed_date, 
                                               status_id, complaint_type_id, location_id)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    batch
                )
                conn.commit()
                stats['success'] += len(batch)
            except psycopg2.Error as e:
                # Erro SQL no batch final: fazer rollback e limpar cache
                conn.rollback()
                # Limpar caches para evitar usar IDs que podem ter sido revertidos
                status_cache.clear()
                complaint_type_cache.clear()
                stats['errors'] += len(batch)
                stats['errors_details'].append(f"Batch final: Erro - {str(e)}")
    
    print("\n" + "=" * 80)
    print("RELATÓRIO DE IMPORTAÇÃO")
    print("=" * 80)
    print(f"Total processado: {stats['total']:,}")
    print(f"Sucesso: {stats['success']:,}")
    print(f"Erros: {stats['errors']:,}")
    if stats['total'] > 0:
        print(f"Taxa de sucesso: {(stats['success']/stats['total']*100):.2f}%")
    
    if stats['errors_details']:
        print(f"\nPrimeiros 10 erros:")
        for error in stats['errors_details'][:10]:
            print(f"  {error}")
    
    return stats['errors'] == 0


def main():
    parser = argparse.ArgumentParser(description='Importa dados simplificados do CSV para PostgreSQL v2')
    parser.add_argument('--csv', help='Arquivo CSV de entrada')
    parser.add_argument('--host', default='localhost', help='Host do PostgreSQL')
    parser.add_argument('--port', type=int, default=5434, help='Porta do PostgreSQL')
    parser.add_argument('--db', default='nyc_complaints', help='Nome do banco de dados')
    parser.add_argument('--user', default='postgres', help='Usuário do PostgreSQL')
    parser.add_argument('--password', default='postgres', help='Senha do PostgreSQL')
    parser.add_argument('--batch-size', type=int, default=100, help='Tamanho do batch para inserção')
    
    args = parser.parse_args()
    
    # Resolver caminho do CSV
    if args.csv:
        csv_path = os.path.abspath(args.csv)
    else:
        # Tentar encontrar CSV em v2/data/
        script_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.join(script_dir, 'data')
        csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')] if os.path.exists(data_dir) else []
        if csv_files:
            csv_path = os.path.join(data_dir, csv_files[0])
            print(f"Usando CSV encontrado: {csv_files[0]}")
        else:
            print("ERRO: Nenhum arquivo CSV encontrado. Use --csv para especificar.")
            sys.exit(1)
    
    if not os.path.exists(csv_path):
        print(f"ERRO: Arquivo CSV não encontrado: {csv_path}")
        sys.exit(1)
    
    # Conectar ao banco
    print(f"Conectando ao banco de dados {args.db} em {args.host}:{args.port}...")
    conn = get_db_connection(args.host, args.port, args.db, args.user, args.password)
    
    try:
        # Importar dados
        success = import_csv(conn, csv_path, args.batch_size)
        
        if success:
            print("\n✓ Importação concluída com sucesso!")
            sys.exit(0)
        else:
            print("\n✗ Importação concluída com erros.")
            sys.exit(1)
            
    finally:
        conn.close()


if __name__ == '__main__':
    main()
