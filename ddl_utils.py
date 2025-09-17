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
    """Ejecuta el contenido de un archivo .sql (CREATE TABLE IF NOT EXISTS ...)."""
    if not os.path.isfile(ddl_file):
        raise FileNotFoundError(f"DDL no encontrado: {ddl_file}")

    sql_text = open(ddl_file, "r", encoding="utf-8").read()
    statements = [s.strip() for s in sql_text.split(";") if s.strip()]
    if not statements:
        raise ValueError(f"Archivo DDL vacío: {ddl_file}")

    conn = _conn()
    try:
        with conn.cursor() as cur:
            for stmt in statements:
                cur.execute(stmt)
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
