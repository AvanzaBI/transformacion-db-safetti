import pandas as pd
from parsing import extract_date_from_filename

def add_fecha_operacion(df: pd.DataFrame, src_path: str) -> pd.DataFrame:
    """
    Añade solo la columna 'fecha_operacion', obtenida del nombre del archivo.
    No modifica el resto de columnas.
    """
    fecha = extract_date_from_filename(src_path)
    df["fecha_operacion"] = fecha
    df["fecha_operacion"] = pd.to_datetime(df["fecha_operacion"], errors="coerce", dayfirst=True).dt.date
    return df