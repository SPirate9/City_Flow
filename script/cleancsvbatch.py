import csv, boto3, io, unicodedata
from datetime import datetime

IN_PATH  = "chunk_0.csv"
OUT_PATH = "traffic_clean.csv"
BUCKET   = "cityflow-clean-paris"

s3 = boto3.client("s3")

def normalize(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return s.strip().lower().replace(" ", "_")

def float_or_none(x):
    try: return float(str(x).replace(",", "."))
    except: return None

def parse_geo_point(val):
    if not val or "," not in val:
        return None, None
    try:
        lat, lon = val.split(",", 1)
        return float(lat), float(lon)
    except:
        return None, None

def grid(lat, lon):
    if lat is None or lon is None:
        return ""
    return f"{round(lat, 4)}|{round(lon, 4)}"

def detect_delimiter(file_path):
    with open(file_path, "r", encoding="utf-8") as f:
        first_line = f.readline()
        if ";" in first_line:
            return ";"
        elif "," in first_line:
            return ","
        else:
            return "\t"

def clean_csv():
    delim = detect_delimiter(IN_PATH)
    print(f"→ Délimiteur détecté : '{delim}'")

    with open(IN_PATH, newline="", encoding="utf-8") as fin, \
         open(OUT_PATH, "w", encoding="utf-8", newline="") as fout:

        reader = csv.reader(fin, delimiter=delim)
        headers = [normalize(h) for h in next(reader)]
        idx = {h: i for i, h in enumerate(headers)}

        cols_out = ["segment_id","ts","flow_per_min","occupancy","status","lat","lon","grid_1e4","source"]
        writer = csv.DictWriter(fout, fieldnames=cols_out, delimiter=";")
        writer.writeheader()

        count = 0
        for row in reader:
            seg = row[idx.get("identifiant_arc", -1)] if "identifiant_arc" in idx else ""
            ts  = row[idx.get("date_et_heure_de_comptage", -1)] if "date_et_heure_de_comptage" in idx else ""
            fh  = float_or_none(row[idx.get("debit_horaire", -1)] if "debit_horaire" in idx else "")
            occ = float_or_none(row[idx.get("taux_d_occupation", -1)] if "taux_d_occupation" in idx else "")
            status = row[idx.get("etat_trafic", -1)] if "etat_trafic" in idx else ""
            gp = row[idx.get("geo_point_2d", -1)] if "geo_point_2d" in idx else ""
            lat, lon = parse_geo_point(gp)

            if not seg or not ts:
                continue

            writer.writerow({
                "segment_id": seg,
                "ts": ts,
                "flow_per_min": round(fh/60,2) if fh else "",
                "occupancy": occ or "",
                "status": status,
                "lat": lat or "",
                "lon": lon or "",
                "grid_1e4": grid(lat, lon),
                "source": "csv_batch"
            })
            count += 1

    print(f"✅ Nettoyage terminé ({count} lignes).")
    return OUT_PATH

if __name__ == "__main__":
    out = clean_csv()
    now = datetime.utcnow().strftime("%Y/%m")
    key = f"clean/traffic/{now}/traffic_clean.csv"
    s3.upload_file(out, BUCKET, key, ExtraArgs={"ContentType": "text/csv"})
    print(f"🚀 Envoyé vers s3://{BUCKET}/{key}")

