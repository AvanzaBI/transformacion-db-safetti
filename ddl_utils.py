# ddl_utils.py
import os
import pymysql
from pymysql.constants import CLIENT

SSL_CA = os.path.expanduser("~/DigiCertGlobalRootG2.crt.pem")

def _conn():
    host = os.getenv("MYSQL_HOST")
    port = int(os.getenv("MYSQL_PORT", "3306"))
    user = os.getenv("MYSQL_USER")
    pwd  = os.getenv("MYSQL_PASSWORD")
    db   = os.getenv("MYSQL_DATABASE")

    missing = [k for k,v in {
        "MYSQL_HOST":host, "MYSQL_PORT":port, "MYSQL_USER":user,
        "MYSQL_PASSWORD":pwd, "MYSQL_DATABASE":db
    }.items() if v in (None, "")]
    if missing:
        raise RuntimeError(f"Faltan variables en .env: {missing}")

    return pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=pwd,
        database=db,
        charset="utf8mb4",
        client_flag=CLIENT.LOCAL_FILES,
        local_infile=1,
        ssl={"ca": SSL_CA},
        autocommit=True,
    )

def ensure_table_from_file(ddl_file: str):
<<<<<<< HEAD
    """Ejecuta el contenido de un archivo .sql (CREATE TABLE IF NOT EXISTS ...)."""
    if not os.path.isfile(ddl_file):
        raise FileNotFoundError(f"DDL no encontrado: {ddl_file}")

    sql_text = open(ddl_file, "r", encoding="utf-8").read()
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    if not statements:
        raise ValueError(f"Archivo DDL vacío: {ddl_file}")
=======
    """
    Reemplaza la tabla con el DDL indicado.
    - Lee el archivo .sql (debe contener un CREATE TABLE completo).
    - Ejecuta DROP TABLE IF EXISTS <nombre_tabla>.
    - Ejecuta el CREATE TABLE.
    """
    ddl_path = os.path.abspath(ddl_file)
    if not os.path.exists(ddl_path):
        raise FileNotFoundError(f"Archivo DDL no encontrado: {ddl_path}")

    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read().strip()

    # Extraer el nombre de la tabla del CREATE (simple parseo)
    import re
    ddl_clean = " ".join(ddl_sql.split()).lower()

# Busca el patrón CREATE TABLE [IF NOT EXISTS] nombre
    m = re.search(
        r'create\s+table\s+(if\s+not\s+exists\s+)?`?([a-z0-9_.]+)`?',
        ddl_clean,
        re.IGNORECASE,
    )
    if not m:
        raise ValueError("No se pudo extraer el nombre de la tabla del DDL")
    
    table_name = m.group(2)
>>>>>>> f116724 (init: estructura Safetti ETL)

    conn = _conn()
    try:
        with conn.cursor() as cur:
<<<<<<< HEAD
            for stmt in statements:
                cur.execute(stmt)
=======
            print(f"[DDL] Dropping and creating table {table_name} from {ddl_file}")
            cur.execute(f"DROP TABLE IF EXISTS {table_name}")
            cur.execute(ddl_sql)
        conn.commit()
>>>>>>> f116724 (init: estructura Safetti ETL)
    finally:
        conn.close()

def truncate_table(table: str):
    """TRUNCATE TABLE {table}"""
    if not table or not isinstance(table, str):
        raise ValueError("Nombre de tabla inválido para TRUNCATE")
    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"TRUNCATE TABLE {table}")
    finally:
        conn.close()

def ensure_index_exists(table: str, index_name: str, columns: list[str], unique: bool = False):
    """
    Crea el índice si no existe, consultando information_schema.statistics.
    columns: lista de columnas en orden.
    """
    if not table or not index_name or not columns:
        raise ValueError("Parámetros inválidos para ensure_index_exists")

    conn = _conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 1
                FROM information_schema.statistics
                WHERE table_schema = DATABASE()
                  AND table_name = %s
                  AND index_name = %s
                LIMIT 1
            """, (table.split(".")[-1], index_name))  # soporta 'schema.table' o 'table'
            exists = cur.fetchone() is not None

            if not exists:
                cols_sql = ", ".join(f"`{c}`" for c in columns)
                unique_sql = "UNIQUE " if unique else ""
                cur.execute(f"CREATE {unique_sql}INDEX {index_name} ON {table} ({cols_sql})")
    finally:
        conn.close()
