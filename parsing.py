# parsing.py
import re
import datetime as dt

SPANISH_MONTHS = {
    "ene":1,"feb":2,"mar":3,"abr":4,"may":5,"jun":6,
    "jul":7,"ago":8,"sep":9,"oct":10,"nov":11,"dic":12,
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"setiembre":9,"octubre":10,
    "noviembre":11,"diciembre":12,
}

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

    return None
