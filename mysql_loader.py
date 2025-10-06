# mysql_loader.py
import os
<<<<<<< HEAD
import tempfile
=======
import math
import tempfile
from typing import List, Optional, Tuple
import numpy as np
import datetime as dt
>>>>>>> f116724 (init: estructura Safetti ETL)
import pandas as pd
import pymysql
from pymysql.constants import CLIENT

# Usa el certificado que descargaste antes
SSL_CA = os.path.expanduser("~/DigiCertGlobalRootG2.crt.pem")

<<<<<<< HEAD
=======
def _coerce_for_mysql(value):
    """Convierte valores de pandas/numpy a tipos nativos seguros para PyMySQL."""
    # NaN / NA -> None
    if value is None:
        return None
    # pandas NA scalars
    try:
        import pandas as pd  # por si acaso
        if pd.isna(value):
            return None
    except Exception:
        pass
    # numpy scalars -> python
    if isinstance(value, (np.generic,)):
        value = value.item()
    # Timestamps / datetimes
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp):
            # to_pydatetime conserva tz-naive/aware
            return value.to_pydatetime()
    except Exception:
        pass
    if isinstance(value, (dt.date, dt.datetime)):
        return value  # PyMySQL los maneja
    return value

>>>>>>> f116724 (init: estructura Safetti ETL)
def get_mysql_conn():
    """
    Devuelve una conexión MySQL con:
    - SSL (requerido por Azure MySQL cuando require_secure_transport=ON)
    - LOCAL INFILE habilitado (para LOAD DATA LOCAL INFILE)
    """
<<<<<<< HEAD
    return pymysql.connect(
=======
    conn = pymysql.connect(
>>>>>>> f116724 (init: estructura Safetti ETL)
        host=os.getenv("MYSQL_HOST"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER"),
        password=os.getenv("MYSQL_PASSWORD"),
        database=os.getenv("MYSQL_DATABASE"),
        charset="utf8mb4",
        client_flag=CLIENT.LOCAL_FILES,
        local_infile=1,
        ssl={"ca": SSL_CA},
    )

<<<<<<< HEAD
def load_batch_to_mysql(
    df: pd.DataFrame,
    table: str,
    column_list: list[str] | None = None,
    *,
    line_ending: str = "\n"
) -> int:
    """
    Carga masiva un DataFrame a MySQL usando LOAD DATA LOCAL INFILE.

    Parámetros:
      - df: DataFrame a insertar (no se modifica).
      - table: tabla destino (p.ej. 'safetti.inventario_por_bodega').
      - column_list: lista opcional para forzar el orden de columnas en la carga.
                     Útil si el orden del DF no coincide exactamente con la tabla.
      - line_ending: '\n' (Linux) o '\r\n' (Windows). Ajusta si hiciera falta.

    Retorna:
      - filas insertadas (rowcount reportado por el servidor).
    """
    if df.empty:
        return 0

    # 1) Escribir CSV temporal
    with tempfile.NamedTemporaryFile("w", suffix=".csv", delete=False) as tmp:
        tmp_path = tmp.name
        df.to_csv(tmp, index=False, encoding="utf-8-sig", lineterminator=line_ending)

    try:
        # 2) Conectar con SSL + LOCAL INFILE
        conn = get_mysql_conn()
        cur = conn.cursor()

        # 3) Preparar columna(s) si se especificó un orden
        if column_list:
            cols_sql = "(" + ",".join(f"`{c}`" for c in column_list) + ")"
        else:
            cols_sql = ""  # MySQL usará el orden del CSV

        # 4) Ejecutar LOAD DATA LOCAL INFILE
        sql = f"""
        LOAD DATA LOCAL INFILE %s
        INTO TABLE {table}
        CHARACTER SET utf8mb4
        FIELDS TERMINATED BY ',' ENCLOSED BY '"'
        LINES TERMINATED BY %s
        IGNORE 1 LINES
        {cols_sql}
        """

        cur.execute(sql, (tmp_path, line_ending))
        conn.commit()
        return cur.rowcount

    finally:
        # Cerrar y limpiar
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
        try:
            os.remove(tmp_path)
        except Exception:
            pass

=======
    # Debug opcional: ver sql_mode de la sesión
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT @@sql_mode;")
            print("[DEBUG sql_mode loader]", cur.fetchone())
    except Exception:
        pass

    return conn


def _split_schema_table(full_name: str) -> Tuple[Optional[str], str]:
    """
    Separa 'schema.table' o devuelve (None, 'table') si no hay schema.
    """
    if "." in full_name:
        schema, table = full_name.split(".", 1)
        return schema, table
    return None, full_name


def _quote_ident(ident: str) -> str:
    """Envuelve un identificador con backticks, escapando backticks internos."""
    return "`" + ident.replace("`", "``") + "`"


def _full_table_name(table: str) -> str:
    schema, tbl = _split_schema_table(table)
    if schema:
        return f"{_quote_ident(schema)}.{_quote_ident(tbl)}"
    return _quote_ident(tbl)


def _df_to_temp_csv(df: pd.DataFrame, line_ending: str = "\n") -> str:
    r"""
    Escribe un CSV temporal:
    - sin índice
    - NaN/NaT -> \\N (MySQL lo interpreta como NULL)
    - separador coma, con comillas dobles para campos con separadores
    - newline configurable
    Devuelve la ruta del archivo temporal.
    """
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8")
    df.to_csv(
        tmp,
        index=False,
        encoding="utf-8",
        lineterminator=line_ending,
        na_rep="\\N",  # <-- MUY IMPORTANTE: doble backslash para escribir \N literal
    )
    path = tmp.name
    tmp.close()
    return path


def _insert_executemany(
    conn,
    df: pd.DataFrame,
    table: str,
    column_list: List[str],
    chunk_size: int = 10000,
) -> int:
    """
    Inserta usando INSERT ... VALUES (%s, ...) con executemany por lotes.
    Convierte NaN a None y numpy/pandas scalars a tipos nativos.
    """
    total = 0
    cols_sql = ", ".join(_quote_ident(c) for c in column_list)
    placeholders = ", ".join(["%s"] * len(column_list))
    sql = f"INSERT INTO {_full_table_name(table)} ({cols_sql}) VALUES ({placeholders})"

    n = len(df)
    if n == 0:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            "SET SESSION sql_mode = CONCAT_WS(',', @@sql_mode, "
            "'STRICT_TRANS_TABLES','NO_ZERO_DATE','NO_ZERO_IN_DATE')"
        )

        steps = math.ceil(n / chunk_size)
        for i in range(steps):
            start = i * chunk_size
            end = min(start + chunk_size, n)

            # ¡CLAVE!: pasar a object para que None no se convierta de vuelta a NaN
            sub = df.iloc[start:end][column_list].copy().astype(object)

            # Reemplazar cualquier NaN/NA por None
            sub = sub.where(pd.notna(sub), None)

            # Convertir fila por fila a tuplas con tipos seguros
            rows = []
            for tup in sub.itertuples(index=False, name=None):
                rows.append(tuple(_coerce_for_mysql(v) for v in tup))

            if rows:
                cur.executemany(sql, rows)
                total += (end - start)

    conn.commit()
    return total


def load_batch_to_mysql(
    df: pd.DataFrame,
    table: str,
    column_list: Optional[List[str]] = None,
    *,
    use_load_data_infile: bool = False,
    line_ending: str = "\n",
) -> int:
    r"""
    Carga masiva un DataFrame a MySQL.

    - Si use_load_data_infile=True: intenta usar LOAD DATA LOCAL INFILE (rápido).
      Si falla (políticas, permisos, etc.), hace fallback automático a executemany.

    - Si use_load_data_infile=False: usa directamente executemany en lotes.

    Retorna número de filas insertadas.
    """
    if df is None or df.empty:
        return 0

    # Columnas a insertar
    if column_list:
        missing = [c for c in column_list if c not in df.columns]
        if missing:
            raise ValueError(f"Faltan columnas requeridas en DataFrame: {missing}")
        df_to_insert = df[column_list].copy()
    else:
        column_list = list(df.columns)
        df_to_insert = df.copy()

    conn = get_mysql_conn()

    # Camino turbo: LOAD DATA LOCAL INFILE
    if use_load_data_infile:
        tmp_path = _df_to_temp_csv(df_to_insert, line_ending=line_ending)
        try:
            cols_sql = "(" + ",".join(_quote_ident(c) for c in column_list) + ")"
            sql = f"""
                LOAD DATA LOCAL INFILE %s
                INTO TABLE {_full_table_name(table)}
                CHARACTER SET utf8mb4
                FIELDS TERMINATED BY ',' ENCLOSED BY '"'
                ESCAPED BY '\\\\'
                LINES TERMINATED BY %s
                IGNORE 1 LINES
                {cols_sql}
            """
            with conn.cursor() as cur:
                cur.execute(
                    "SET SESSION sql_mode = CONCAT_WS(',', @@sql_mode, "
                    "'STRICT_TRANS_TABLES','NO_ZERO_DATE','NO_ZERO_IN_DATE')"
                )
                cur.execute(sql, (tmp_path, line_ending))
            conn.commit()
            return len(df_to_insert)
        except Exception as e:
            print(f"    ⚠ LOAD DATA LOCAL INFILE falló, usando executemany. Motivo: {e}")
            # Fallback a executemany
            try:
                total = _insert_executemany(conn, df_to_insert, table, column_list, chunk_size=10000)
                return total
            finally:
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass
        finally:
            # En caso de éxito también limpiamos
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    # Camino estándar: executemany por lotes
    total = _insert_executemany(conn, df_to_insert, table, column_list, chunk_size=10000)
    return total
>>>>>>> f116724 (init: estructura Safetti ETL)
