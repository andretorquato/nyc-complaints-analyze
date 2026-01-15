#!/usr/bin/env python3
import argparse
import subprocess
import sys
import os
import time
from pathlib import Path

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    print("ERRO: psycopg2-binary não está instalado. Execute: pip install -r requirements.txt")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("ERRO: requests não está instalado. Execute: pip install -r requirements.txt")
    sys.exit(1)

# Configurações
PROJECT_ROOT = Path(__file__).parent
DOCKER_COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
POSTGRES_CONTAINER = "nyc_complaints_v2_postgres"
POSTGRES_DB = os.getenv("POSTGRES_DB", "nyc_complaints")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_PORT = 5434

# Caminhos dos scripts
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
DATA_DIR = PROJECT_ROOT / "data"
IMPORT_SCRIPT = PROJECT_ROOT / "import_data.py"
QUERIES_SQL = SCRIPTS_DIR / "queries.sql"
EXPLAIN_ANALYZE_SQL = SCRIPTS_DIR / "explain_analyze.sql"
CREATE_INDEXES_SQL = SCRIPTS_DIR / "create_indexes.sql"


def check_docker() -> bool:
    """Verifica se Docker está rodando"""
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def check_containers() -> dict:
    """Verifica status dos containers"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--format", "{{.Names}}"],
            capture_output=True,
            text=True,
            timeout=5
        )
        running = set(result.stdout.strip().split('\n'))
        return {
            "postgres": POSTGRES_CONTAINER in running,
            "grafana": "nyc_complaints_v2_grafana" in running,
            "pgadmin": "nyc_complaints_v2_pgadmin" in running
        }
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return {"postgres": False, "grafana": False, "pgadmin": False}


def wait_for_postgres(max_wait: int = 60) -> bool:
    """Aguarda PostgreSQL ficar pronto"""
    print("Aguardando PostgreSQL ficar pronto...")
    start_time = time.time()
    
    while time.time() - start_time < max_wait:
        try:
            conn = psycopg2.connect(
                host="localhost",
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                connect_timeout=2
            )
            conn.close()
            print("✓ PostgreSQL está pronto!")
            return True
        except psycopg2.OperationalError:
            time.sleep(2)
            print(".", end="", flush=True)
    
    print("\n✗ Timeout aguardando PostgreSQL")
    return False


def exec_sql(sql: str, container: str = POSTGRES_CONTAINER) -> tuple:
    """Executa SQL no container PostgreSQL"""
    try:
        cmd = [
            "docker", "exec", "-i", container,
            "psql", "-U", POSTGRES_USER, "-d", POSTGRES_DB
        ]
        process = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        stdout, stderr = process.communicate(input=sql)
        
        if process.returncode == 0:
            return True, stdout
        else:
            return False, stderr
    except Exception as e:
        return False, str(e)


def exec_sql_file(sql_file: Path, container: str = POSTGRES_CONTAINER) -> tuple:
    """Executa arquivo SQL no container"""
    if not sql_file.exists():
        return False, f"Arquivo não encontrado: {sql_file}"
    
    try:
        cmd = [
            "docker", "exec", "-i", container,
            "psql", "-U", POSTGRES_USER, "-d", POSTGRES_DB, "-f", "-"
        ]
        with open(sql_file, 'r') as f:
            process = subprocess.Popen(
                cmd,
                stdin=f,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            stdout, stderr = process.communicate()
        
        if process.returncode == 0:
            return True, stdout
        else:
            return False, stderr
    except Exception as e:
        return False, str(e)


def cmd_start():
    """Inicia a aplicação com Docker"""
    print("=" * 80)
    print("Iniciando aplicação NYC 311 v2")
    print("=" * 80)
    
    if not check_docker():
        print("✗ Docker não está rodando. Por favor, inicie o Docker primeiro.")
        sys.exit(1)
    
    print("✓ Docker está rodando")
    
    if not DOCKER_COMPOSE_FILE.exists():
        print(f"✗ Arquivo docker-compose.yml não encontrado em {DOCKER_COMPOSE_FILE}")
        sys.exit(1)
    
    # Iniciar containers
    print("\nIniciando containers...")
    result = subprocess.run(
        ["docker-compose", "-f", str(DOCKER_COMPOSE_FILE), "up", "-d"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"✗ Erro ao iniciar containers: {result.stderr}")
        sys.exit(1)
    
    print("✓ Containers iniciados")
    
    # Aguardar PostgreSQL
    if not wait_for_postgres():
        print("✗ Falha ao aguardar PostgreSQL")
        sys.exit(1)
    
    # Verificar status dos containers
    containers = check_containers()
    print("\n" + "=" * 80)
    print("Status dos Containers:")
    print("=" * 80)
    for name, running in containers.items():
        status = "✓ Rodando" if running else "✗ Parado"
        print(f"  {name:15} {status}")
    
    print("\n" + "=" * 80)
    print("URLs de Acesso:")
    print("=" * 80)
    print("  Grafana:  http://localhost:3031")
    print("  pgAdmin:  http://localhost:5051")
    print("  PostgreSQL: localhost:5434")
    print("\n✓ Aplicação iniciada com sucesso!")


def cmd_import():
    """Importa CSV para o banco"""
    print("=" * 80)
    print("Importação de CSV")
    print("=" * 80)
    
    # Verificar containers
    containers = check_containers()
    if not containers["postgres"]:
        print("✗ Container PostgreSQL não está rodando. Execute 'python cli.py start' primeiro.")
        sys.exit(1)
    
    # Listar CSVs
    if not DATA_DIR.exists():
        print(f"✗ Diretório de dados não encontrado: {DATA_DIR}")
        sys.exit(1)
    
    csv_files = sorted([f for f in DATA_DIR.glob("*.csv")])
    if not csv_files:
        print("✗ Nenhum arquivo CSV encontrado em v2/data/")
        sys.exit(1)
    
    print("\nArquivos CSV disponíveis:")
    for i, csv_file in enumerate(csv_files, 1):
        print(f"  {i}. {csv_file.name}")
    
    # Seleção interativa
    while True:
        try:
            choice = input(f"\nSelecione o arquivo (1-{len(csv_files)}): ").strip()
            idx = int(choice) - 1
            if 0 <= idx < len(csv_files):
                selected_csv = csv_files[idx]
                break
            else:
                print("Opção inválida. Tente novamente.")
        except (ValueError, KeyboardInterrupt):
            print("\nOperação cancelada.")
            sys.exit(0)
    
    print(f"\n✓ Arquivo selecionado: {selected_csv.name}")
    
    # Confirmar limpeza
    print("\n⚠️  ATENÇÃO: Todos os dados atuais serão deletados!")
    confirm = input("Deseja continuar? (s/N): ").strip().lower()
    if confirm != 's':
        print("Operação cancelada.")
        sys.exit(0)
    
    # Limpar dados
    print("Limpando dados do banco...")
    truncate_sql = """
    TRUNCATE TABLE complaints CASCADE;
    TRUNCATE TABLE locations CASCADE;
    TRUNCATE TABLE complaint_types CASCADE;
    TRUNCATE TABLE statuses CASCADE;
    """
    success, output = exec_sql(truncate_sql)
    if not success:
        print(f"✗ Erro ao limpar dados: {output}")
        sys.exit(1)
    print("✓ Dados limpos")
    
    # Importar CSV
    print(f"\nImportando {selected_csv.name}...")
    csv_abs_path = selected_csv.absolute()
    
    # Copiar CSV para o container
    print("Copiando CSV para o container...")
    copy_cmd = [
        "docker", "cp", str(csv_abs_path),
        f"{POSTGRES_CONTAINER}:/tmp/import.csv"
    ]
    result = subprocess.run(copy_cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"✗ Erro ao copiar CSV: {result.stderr}")
        sys.exit(1)
    
    # Executar script de importação
    print("Executando importação...")
    import_cmd = [
        "docker", "exec", "-e", f"PGPASSWORD={POSTGRES_PASSWORD}",
        POSTGRES_CONTAINER,
        "python3", "/scripts/import_data.py",
        "--csv", "/tmp/import.csv",
        "--host", "localhost",
        "--port", "5432",
        "--db", POSTGRES_DB,
        "--user", POSTGRES_USER,
        "--password", POSTGRES_PASSWORD
    ]
    
    process = subprocess.Popen(
        import_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1
    )
    
    # Exibir output em tempo real
    for line in process.stdout:
        print(line, end='')
    
    process.wait()
    
    if process.returncode == 0:
        print("\n✓ Importação concluída com sucesso!")
    else:
        print("\n✗ Erro na importação")
        sys.exit(1)


def cmd_test_query():
    """Testa query com EXPLAIN ANALYZE"""
    print("=" * 80)
    print("Teste de Queries com EXPLAIN ANALYZE")
    print("=" * 80)
    
    # Verificar containers
    containers = check_containers()
    if not containers["postgres"]:
        print("✗ Container PostgreSQL não está rodando. Execute 'python cli.py start' primeiro.")
        sys.exit(1)
    
    # Verificar se arquivo existe
    if not EXPLAIN_ANALYZE_SQL.exists():
        print(f"✗ Arquivo não encontrado: {EXPLAIN_ANALYZE_SQL}")
        sys.exit(1)
    
    # Perguntar se deve executar com ou sem índices
    print("\nExecutar:")
    print("  1. Com índices")
    print("  2. Sem índices")
    
    while True:
        try:
            idx_choice = input("Escolha (1-2): ").strip()
            if idx_choice == '1':
                # Garantir que índices existem
                print("\nCriando índices...")
                success, output = exec_sql_file(CREATE_INDEXES_SQL)
                if not success:
                    print(f"✗ Erro ao criar índices: {output}")
                    sys.exit(1)
                print("✓ Índices criados")
                
                # Atualizar estatísticas após criar índices
                print("Atualizando estatísticas do planner...")
                analyze_sql = """
                ANALYZE complaints;
                ANALYZE locations;
                ANALYZE complaint_types;
                ANALYZE statuses;
                """
                success, output = exec_sql(analyze_sql)
                if not success:
                    print(f"⚠️  Aviso: Erro ao atualizar estatísticas: {output}")
                else:
                    print("✓ Estatísticas atualizadas")
                
                # Verificar índices criados
                verify_sql = """
                SELECT indexname, tablename 
                FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND indexname LIKE 'idx_%'
                ORDER BY tablename, indexname;
                """
                success, output = exec_sql(verify_sql)
                if success and output.strip():
                    print("\nÍndices criados:")
                    for line in output.strip().split('\n'):
                        if line.strip() and 'indexname' not in line.lower() and '---' not in line:
                            print(f"  {line.strip()}")
                break
            elif idx_choice == '2':
                # Remover índices
                print("\nRemovendo índices...")
                drop_sql = """
                DROP INDEX IF EXISTS idx_complaints_created_date;
                DROP INDEX IF EXISTS idx_complaints_status_id;
                DROP INDEX IF EXISTS idx_complaints_location_id;
                DROP INDEX IF EXISTS idx_complaints_complaint_type_id;
                DROP INDEX IF EXISTS idx_locations_borough;
                DROP INDEX IF EXISTS idx_complaints_complaint_type_location_id;
                """
                success, output = exec_sql(drop_sql)
                if not success:
                    print(f"✗ Erro ao remover índices: {output}")
                    sys.exit(1)
                print("✓ Índices removidos")
                
                # Atualizar estatísticas após remover índices
                print("Atualizando estatísticas do planner...")
                analyze_sql = """
                ANALYZE complaints;
                ANALYZE locations;
                ANALYZE complaint_types;
                ANALYZE statuses;
                """
                success, output = exec_sql(analyze_sql)
                if not success:
                    print(f"⚠️  Aviso: Erro ao atualizar estatísticas: {output}")
                else:
                    print("✓ Estatísticas atualizadas")
                
                # Verificar que índices foram removidos
                verify_sql = """
                SELECT indexname, tablename 
                FROM pg_indexes 
                WHERE schemaname = 'public' 
                AND indexname IN (
                    'idx_complaints_created_date',
                    'idx_complaints_status_id',
                    'idx_complaints_location_id',
                    'idx_complaints_complaint_type_id',
                    'idx_locations_borough',
                    'idx_complaints_complaint_type_location_id'
                )
                ORDER BY tablename, indexname;
                """
                success, output = exec_sql(verify_sql)
                remaining_indexes = [line.strip() for line in output.strip().split('\n') 
                                    if line.strip() and 'indexname' not in line.lower() and '---' not in line]
                if remaining_indexes:
                    print(f"\n⚠️  Aviso: {len(remaining_indexes)} índice(s) ainda existem:")
                    for idx in remaining_indexes:
                        print(f"  {idx}")
                else:
                    print("\n✓ Todos os índices foram removidos com sucesso")
                break
            else:
                print("Opção inválida. Tente novamente.")
        except KeyboardInterrupt:
            print("\nOperação cancelada.")
            sys.exit(0)
    
    # Executar EXPLAIN ANALYZE
    print(f"\nExecutando EXPLAIN ANALYZE...")
    print("=" * 80)
    
    success, output = exec_sql_file(EXPLAIN_ANALYZE_SQL)
    
    if success:
        print(output)
        print("=" * 80)
    else:
        print(f"✗ Erro ao executar queries: {output}")
        sys.exit(1)


def cmd_reset():
    """Reseta banco de dados"""
    print("=" * 80)
    print("Resetar Banco de Dados")
    print("=" * 80)
    
    print("⚠️  ATENÇÃO: Isso irá remover todos os containers e volumes!")
    print("Todos os dados serão perdidos!")
    confirm = input("Deseja continuar? (s/N): ").strip().lower()
    if confirm != 's':
        print("Operação cancelada.")
        return
    
    print("\nParando e removendo containers...")
    result = subprocess.run(
        ["docker-compose", "-f", str(DOCKER_COMPOSE_FILE), "down", "-v"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"✗ Erro ao parar containers: {result.stderr}")
        sys.exit(1)
    
    print("✓ Containers removidos")
    
    print("\nRecriando containers...")
    result = subprocess.run(
        ["docker-compose", "-f", str(DOCKER_COMPOSE_FILE), "up", "-d"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        print(f"✗ Erro ao recriar containers: {result.stderr}")
        sys.exit(1)
    
    print("✓ Containers recriados")
    
    # Aguardar PostgreSQL
    if not wait_for_postgres():
        print("✗ Falha ao aguardar PostgreSQL")
        sys.exit(1)
    
    print("\n✓ Banco de dados resetado com sucesso!")
    print("O banco está no estado inicial (tabelas criadas, sem dados).")


def cmd_download():
    """Baixa dados CSV da API NYC 311"""
    print("=" * 80)
    print("Download de Dados CSV - NYC 311")
    print("=" * 80)
    
    # Solicitar quantidade de linhas
    while True:
        try:
            quantidade_input = input("\nDigite a quantidade de linhas que deseja baixar: ").strip()
            quantidade = int(quantidade_input)
            
            if quantidade <= 0:
                print("✗ A quantidade deve ser um número positivo. Tente novamente.")
                continue
            
            break
        except ValueError:
            print("✗ Entrada inválida. Digite um número inteiro positivo.")
        except KeyboardInterrupt:
            print("\nOperação cancelada.")
            sys.exit(0)
    
    # Garantir que o diretório data existe
    if not DATA_DIR.exists():
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        print(f"✓ Diretório criado: {DATA_DIR}")
    
    # Construir URL
    url = f"https://data.cityofnewyork.us/resource/erm2-nwe9.csv?$limit={quantidade}"
    filename = f"{quantidade}_rows.csv"
    filepath = DATA_DIR / filename
    
    print(f"\nBaixando {quantidade} linhas da API NYC 311...")
    print(f"URL: {url}")
    print(f"Salvando em: {filepath}")
    
    try:
        # Fazer download
        response = requests.get(url, timeout=3000, stream=True)
        response.raise_for_status()
        
        # Salvar arquivo
        total_size = 0
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    total_size += len(chunk)
        
        # Verificar se o arquivo foi salvo corretamente
        if filepath.exists() and filepath.stat().st_size > 0:
            file_size_mb = total_size / (1024 * 1024)
            print(f"\n✓ Download concluído com sucesso!")
            print(f"  Arquivo: {filename}")
            print(f"  Tamanho: {file_size_mb:.2f} MB")
            print(f"  Localização: {filepath}")
        else:
            print("\n✗ Erro: Arquivo foi baixado mas está vazio ou não foi salvo corretamente.")
            sys.exit(1)
            
    except requests.exceptions.RequestException as e:
        print(f"\n✗ Erro ao fazer download: {e}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Status code: {e.response.status_code}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n\nOperação cancelada pelo usuário.")
        # Remover arquivo parcial se existir
        if filepath.exists():
            filepath.unlink()
            print("Arquivo parcial removido.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Erro inesperado: {e}")
        # Remover arquivo parcial se existir
        if filepath.exists():
            filepath.unlink()
        sys.exit(1)


def main():
    """Função principal do CLI"""
    parser = argparse.ArgumentParser(
        description="CLI Simplificado para Gerenciamento do Projeto NYC 311 v2",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Comandos disponíveis')
    
    # Comando start
    subparsers.add_parser('start', help='Inicia a aplicação com Docker')
    
    # Comando download
    subparsers.add_parser('download', help='Baixa dados CSV da API NYC 311')
    
    # Comando import
    subparsers.add_parser('import', help='Importa CSV para o banco de dados')
    
    # Comando test-query
    subparsers.add_parser('test-query', help='Testa query com EXPLAIN ANALYZE')
    
    # Comando reset
    subparsers.add_parser('reset', help='Reseta banco de dados (remove containers e volumes)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(1)
    
    try:
        if args.command == 'start':
            cmd_start()
        elif args.command == 'download':
            cmd_download()
        elif args.command == 'import':
            cmd_import()
        elif args.command == 'test-query':
            cmd_test_query()
        elif args.command == 'reset':
            cmd_reset()
    except KeyboardInterrupt:
        print("\n\nOperação cancelada pelo usuário.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Erro inesperado: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
