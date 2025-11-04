import os, io, csv, json, ast, unicodedata, boto3
from datetime import datetime, timezone

SRC_BUCKET = os.environ.get("SRC_BUCKET", "cityflow-raw-paris")
DST_BUCKET = os.environ.get("DST_BUCKET", "cityflow-clean-paris")
s3 = boto3.client("s3")

OUT_COLS = ["work_id","title","owner","road_name","start_ts","end_ts",
            "impact","impact_detail","level","status","lat","lon","grid_1e4","source"]

def _norm(s: str) -> str:
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s).encode("ascii","ignore").decode("ascii")
    return s.strip().lower().replace(" ", "_")

def _to_iso_date(s: str) -> str:
    if not s: return ""
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s.strip(), fmt).date().isoformat()
        except Exception:
            continue
    return s.strip()

def _parse_geo_point_any(val):
    if not val: return None, None
    if isinstance(val, dict):
        try: return float(val.get("lat")), float(val.get("lon"))
        except Exception: return None, None
    if isinstance(val, str):
        s = val.strip()
        if "," in s and not s.startswith("{"):
            try:
                a, b = s.split(",", 1); return float(a), float(b)
            except Exception: pass
        if s.startswith("{"):
            try: return _parse_geo_point_any(json.loads(s))
            except Exception:
                try: return _parse_geo_point_any(ast.literal_eval(s))
                except Exception: return None, None
    return None, None

def _parse_geo_shape_any(val):
    if not val: return None
    if isinstance(val, dict):
        if val.get("type") == "Feature": return val.get("geometry")
        if "type" in val and "coordinates" in val: return val
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s: return None
        try: return _parse_geo_shape_any(json.loads(s))
        except Exception:
            try: return _parse_geo_shape_any(ast.literal_eval(s))
            except Exception: return None
    return None

def _centroid_from_geometry(geom):
    if not isinstance(geom, dict): return None, None
    t = (geom.get("type") or "").lower()
    coords = geom.get("coordinates")
    def avg(pts):
        if not pts: return None, None
        xs = ys = 0.0; n = 0
        for p in pts:
            if not isinstance(p, (list, tuple)) or len(p) < 2: continue
            xs += p[0]; ys += p[1]; n += 1
        if n == 0: return None, None
        lon = xs/n; lat = ys/n
        return lat, lon
    if t == "point":
        try: lon, lat = coords; return lat, lon
        except Exception: return None, None
    if t == "linestring": return avg(coords)
    if t == "polygon":    return avg(coords[0] if coords else None)
    if t == "multipolygon":
        try: return avg(coords[0][0])
        except Exception: return None, None
    return None, None

def _grid(lat, lon, scale=4):
    if lat is None or lon is None: return ""
    return f"{round(lat,scale)}|{round(lon,scale)}"

def _open_bytes(bucket, key) -> bytes:
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()

def _detect_type(key: str, body: bytes) -> str:
    kl = key.lower()
    if kl.endswith(".json"): return "json"
    if kl.endswith(".csv"):  return "csv"
    head = body[:200].decode("utf-8", errors="ignore").lstrip()
    return "json" if (head.startswith("{") or head.startswith("[")) else "csv"

def _rows_from_json(text: str):
    payload = json.loads(text)
    items = payload if isinstance(payload, list) else payload.get("results", [])
    for d in items:
        wid   = d.get("identifiant") or d.get("id") or d.get("_id") or ""
        title = d.get("objet") or d.get("titre") or d.get("description") or ""
        owner = d.get("maitre_ouvrage") or d.get("maitre_d_ouvrage") or ""
        road  = d.get("voie") or d.get("precision_localisation") or ""
        s_ts  = _to_iso_date(d.get("date_debut") or d.get("start_ts") or d.get("start") or "")
        e_ts  = _to_iso_date(d.get("date_fin")   or d.get("end_ts")   or d.get("end")   or "")
        imp   = d.get("impact_circulation") or d.get("impact") or ""
        impd  = d.get("impact_circulation_detail") or d.get("impact_detail") or ""
        lvl   = d.get("niveau_perturbation") or d.get("niveau") or ""
        st    = d.get("statut") or d.get("status") or ""
        lat, lon = _parse_geo_point_any(d.get("geo_point_2d"))
        if lat is None or lon is None:
            geom = _parse_geo_shape_any(d.get("geo_shape"))
            lat, lon = _centroid_from_geometry(geom)
        yield {
            "work_id": wid, "title": title, "owner": owner, "road_name": road,
            "start_ts": s_ts, "end_ts": e_ts, "impact": imp, "impact_detail": impd,
            "level": lvl, "status": st, "lat": lat or "", "lon": lon or "",
            "grid_1e4": _grid(lat, lon), "source": "api_works"
        }

def _rows_from_csv(text: str):
    first = text.splitlines()[0] if text else ""
    delim = ";" if ";" in first else ("," if "," in first else "\t")
    reader = csv.reader(io.StringIO(text), delimiter=delim)
    headers = [_norm(h) for h in next(reader)]
    idx = {h:i for i,h in enumerate(headers)}
    def get(row, *cands):
        for c in cands:
            j = idx.get(c)
            if j is not None and j < len(row):
                return row[j]
        return ""
    for row in reader:
        wid   = get(row, "identifiant","id","_id")
        title = get(row, "objet","titre","description")
        owner = get(row, "maitre_ouvrage","maitre_d_ouvrage")
        road  = get(row, "voie","precision_localisation")
        s_ts  = _to_iso_date(get(row, "date_debut","start_ts","start"))
        e_ts  = _to_iso_date(get(row, "date_fin","end_ts","end"))
        imp   = get(row, "impact_circulation","impact")
        impd  = get(row, "impact_circulation_detail","impact_detail")
        lvl   = get(row, "niveau_perturbation","niveau")
        st    = get(row, "statut","status")
        gp    = get(row, "geo_point_2d")
        shape_raw = get(row, "geo_shape")
        lat, lon = _parse_geo_point_any(gp)
        if lat is None or lon is None:
            geom = _parse_geo_shape_any(shape_raw)
            lat, lon = _centroid_from_geometry(geom)
        yield {
            "work_id": wid or "", "title": title or "", "owner": owner or "", "road_name": road or "",
            "start_ts": s_ts or "", "end_ts": e_ts or "", "impact": imp or "", "impact_detail": impd or "",
            "level": lvl or "", "status": st or "", "lat": lat or "", "lon": lon or "",
            "grid_1e4": _grid(lat, lon), "source": "api_works"
        }

def lambda_handler(event, context):
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key    = rec["s3"]["object"]["key"]
    print(f"[CleanWorks] src: s3://{bucket}/{key}")

    body = _open_bytes(bucket, key)
    ftype = _detect_type(key, body)
    text = body.decode("utf-8", errors="replace")

    # sortie: année/mois + fichier stable (ou mets un timestamp si tu préfères)
    year_month = datetime.now(timezone.utc).strftime("%Y/%m")
    out_key = f"clean/works/{year_month}/works_clean.csv"
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=OUT_COLS, delimiter=";")
    w.writeheader()

    written = 0
    rows = _rows_from_json(text) if ftype == "json" else _rows_from_csv(text)
    for r in rows:
        w.writerow(r); written += 1

    s3.put_object(Bucket=DST_BUCKET, Key=out_key, Body=buf.getvalue().encode("utf-8"),
                  ContentType="text/csv")
    print(f"[CleanWorks] wrote {written} rows → s3://{DST_BUCKET}/{out_key}")
    return {"ok": True, "written": written, "dst": f"s3://{DST_BUCKET}/{out_key}"}
