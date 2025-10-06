# parsing.py
<<<<<<< HEAD
import re
import datetime as dt

SPANISH_MONTHS = {
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
=======
import os
import re
import unicodedata
import datetime as dt
from urllib.parse import unquote

SPANISH_MONTHS = {
    # abreviados
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
    # completos (+ variante setiembre)
>>>>>>> f116724 (init: estructura Safetti ETL)
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
    "noviembre":11,"diciembre":12,
}

<<<<<<< HEAD
DATE_PATTERNS_NUMERIC = [
    r'(?P<y>\d{4})[-_. ](?P<m>\d{1,2})[-_. ](?P<d>\d{1,2})',
    r'(?P<d>\d{1,2})[-_. ](?P<m>\d{1,2})[-_. ](?P<y>\d{4})',
    r'(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})',
    r'(?P<d>\d{2})(?P<m>\d{2})(?P<y>\d{4})',
]

def extract_date_from_filename(path: str) -> dt.date | None:
    base = path.rsplit("/", 1)[-1].rsplit(".", 1)[0].lower().strip()

    # Numéricos
    for pat in DATE_PATTERNS_NUMERIC:
        m = re.search(pat, base)
        if m:
            y, mth, d = int(m["y"]), int(m["m"]), int(m["d"])
            try:
                return dt.date(y, mth, d)
            except ValueError:
                return None

    # “31 enero 2025”
    m = re.search(r'(?P<d>\d{1,2})[-_. ]+(?P<mmmm>[a-záéíóú]+)[-_. ]+(?P<y>\d{4})', base)
    if m:
        d = int(m["d"])
        mmm = (m["mmmm"]
               .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u"))
        if mmm in SPANISH_MONTHS:
            return dt.date(int(m["y"]), SPANISH_MONTHS[mmm], d)

    # “enero-31-2025”
    m = re.search(r'(?P<mmmm>[a-záéíóú]+)[-_. ]+(?P<d>\d{1,2})[-_. ]+(?P<y>\d{4})', base)
    if m:
        d = int(m["d"])
        mmm = (m["mmmm"]
               .replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u"))
        if mmm in SPANISH_MONTHS:
            return dt.date(int(m["y"]), SPANISH_MONTHS[mmm], d)
=======
# Patrones numéricos típicos (con separadores flexibles)
DATE_PATTERNS_NUMERIC = [
    r'(?P<y>\d{4})\D(?P<m>\d{1,2})\D(?P<d>\d{1,2})',   # 2025-08-31 / 2025.08.31 / 2025 08 31
    r'(?P<d>\d{1,2})\D(?P<m>\d{1,2})\D(?P<y>\d{4})',   # 31-08-2025
    r'(?P<y>\d{4})(?P<m>\d{2})(?P<d>\d{2})',           # 20250831
    r'(?P<d>\d{2})(?P<m>\d{2})(?P<y>\d{4})',           # 31082025
]

# Lista para construir el regex de meses (abreviados o completos)
_MES_KEYS = sorted(SPANISH_MONTHS.keys(), key=len, reverse=True)
_MES_REGEX = r'(?:' + '|'.join(map(re.escape, _MES_KEYS)) + r')'

# Textuales con y sin "de" (muy comunes en español)
# 1) 31 agosto 2025 | 31 de agosto de 2025
PAT_DMY_TXT = re.compile(
    rf'(?P<d>\d{{1,2}})\s+(?:de\s+)?(?P<mmmm>{_MES_REGEX})\s+(?:de\s+)?(?P<y>\d{{4}})'
)

# 2) agosto 31 2025 | agosto 31 de 2025
PAT_MDY_TXT = re.compile(
    rf'(?P<mmmm>{_MES_REGEX})\s+(?P<d>\d{{1,2}})\s+(?:de\s+)?(?P<y>\d{{4}})'
)

def _strip_accents(s: str) -> str:
    # Normaliza (NFKD) y remueve diacríticos; útil para “áéíóú” y espacios no estándar
    nfkd = unicodedata.normalize('NFKD', s)
    return ''.join(c for c in nfkd if not unicodedata.combining(c))

def _clean_base_from_path(path: str) -> str:
    # 1) tomar basename (soporta / o \)
    base = os.path.basename(path)
    # 2) decodificar URL (%20)
    base = unquote(base)
    # 3) quitar extensión
    base = re.sub(r'\.[^.]+$', '', base)
    # 4) normalizar unicode y minúsculas
    base = _strip_accents(base).lower().strip()
    # 5) compactar separadores: cualquier whitespace, punto, guion, underscore → espacio
    base = re.sub(r'[\s._-]+', ' ', base)
    return base

def extract_date_from_filename(path: str) -> dt.date | None:
    base = _clean_base_from_path(path)

    # 1) Numéricos primero
    for pat in DATE_PATTERNS_NUMERIC:
        m = re.search(pat, base)
        if m:
            y, mo, d = int(m["y"]), int(m["m"]), int(m["d"])
            try:
                return dt.date(y, mo, d)
            except ValueError:
                return None

    # 2) Textuales: "31 agosto 2025" o "31 de agosto de 2025"
    m = PAT_DMY_TXT.search(base)
    if m:
        d = int(m["d"])
        mon = m["mmmm"]
        mo = SPANISH_MONTHS.get(mon, SPANISH_MONTHS.get(mon[:3]))
        if mo:
            return dt.date(int(m["y"]), mo, d)

    # 3) Textuales: "agosto 31 2025" o "agosto 31 de 2025"
    m = PAT_MDY_TXT.search(base)
    if m:
        d = int(m["d"])
        mon = m["mmmm"]
        mo = SPANISH_MONTHS.get(mon, SPANISH_MONTHS.get(mon[:3]))
        if mo:
            return dt.date(int(m["y"]), mo, d)
>>>>>>> f116724 (init: estructura Safetti ETL)

    return None
