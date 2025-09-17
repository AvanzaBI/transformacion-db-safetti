# mysql_loader.py
import os
import tempfile
import pandas as pd
import pymysql
from pymysql.constants import CLIENT

# Usa el certificado que descargaste antes
SSL_CA = os.path.expanduser("~/DigiCertGlobalRootG2.crt.pem")

def get_mysql_conn():
    """
    Devuelve una conexión MySQL con:
    - SSL (requerido por Azure MySQL cuando require_secure_transport=ON)
    - LOCAL INFILE habilitado (para LOAD DATA LOCAL INFILE)
    """
    return pymysql.connect(
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

