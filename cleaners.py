# cleaners.py
import re
import pandas as pd

def coerce_decimal_column(df: pd.DataFrame, column: str) -> dict:
    """
    Convierte df[column] a float detectando formatos '1,49', '1.234,56', '1,234.56',
    negativos con paréntesis, porcentajes y símbolos. Devuelve un resumen.
    Solo toca la columna indicada; no modifica otras.
    """
    if column not in df.columns:
        return {"column": column, "status": "missing"}

    ser = df[column]

    # si ya es numérico, nada que hacer
    if pd.api.types.is_numeric_dtype(ser):
        return {"column": column, "status": "already_numeric", "dtype": str(ser.dtype)}

    def _norm_one(x):
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return None
        s = str(x).strip()
        if s == "" or s in {"-", "—"}:
            return None

        neg = s.startswith("(") and s.endswith(")")
        if neg:
            s = s[1:-1].strip()

        # quita símbolos comunes
        s = s.replace("$", "").replace(" ", "")

        pct = s.endswith("%")
        if pct:
            s = s[:-1]

        # Si ya es parseable directo
        try:
            val = float(s)
        except ValueError:
            # autodetección de separadores: el último (.,) se asume decimal
            last_dot = s.rfind(".")
            last_com = s.rfind(",")

            if last_dot != -1 and last_com != -1:
                if last_dot > last_com:
                    # '.' decimal, ',' miles
                    s = s.replace(",", "")
                else:
                    # ',' decimal, '.' miles
                    s = s.replace(".", "").replace(",", ".")
            elif last_com != -1:
                # solo comas
                if s.count(",") == 1:
                    s = s.replace(",", ".")
                else:
                    parts = s.split(",")
                    s = "".join(parts[:-1]) + "." + parts[-1]
            elif last_dot != -1:
                # solo puntos
                if s.count(".") > 1:
                    parts = s.split(".")
                    s = "".join(parts[:-1]) + "." + parts[-1]

            try:
                val = float(s)
            except ValueError:
                return None

        if neg:
            val = -val
        if pct:
            val = val / 100.0
        return val

    before_nonnull = int(ser.notna().sum())
    converted = ser.astype(object).map(_norm_one)
    after_nonnull = int(pd.Series(converted).notna().sum())
    changed = int((ser.astype(str).values != pd.Series(converted).astype(str).values).sum())

    df[column] = pd.Series(converted, index=df.index, dtype="float64")
    return {
        "column": column,
        "status": "converted",
        "before_nonnull": before_nonnull,
        "after_nonnull": after_nonnull,
        "changed": changed,
        "dtype": str(df[column].dtype),
    }
