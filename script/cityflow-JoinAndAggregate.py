import os, io, csv, json, math, boto3
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from decimal import Decimal, InvalidOperation
import math


# --- AWS clients ---
s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")

# --- Configs (buckets + rayon de jointure) ---
CLEAN_BUCKET  = os.environ.get("CLEAN_BUCKET",  "cityflow-clean-paris")
REPORT_BUCKET = os.environ.get("REPORT_BUCKET", "cityflow-reports-paris")
DDB_TABLE     = os.environ.get("DDB_TABLE",     "cityflow_metrics")
RADIUS_M      = float(os.environ.get("RADIUS_M", "150"))

table = ddb.Table(DDB_TABLE)

# ---------- UTILS TEMPS / GEO ----------
def parse_dt_or_date(s):
    if not s: return None
    s = s.replace("Z","+00:00")
    try:
        return datetime.fromisoformat(s)
    except:
        try:
            return datetime.fromisoformat(s + "T00:00:00+00:00")
        except:
            return None

def day_hour_from_ts(ts):
    if ts is None: return None, None
    if ts.tzinfo is None: ts = ts.replace(tzinfo=timezone.utc)
    t = ts.astimezone(timezone.utc)
    return t.date().isoformat(), t.hour

def grid1e3(lat, lon):
    if lat is None or lon is None: return ""
    return f"{round(lat,3)}|{round(lon,3)}"

def neighbors(cell):
    if not cell: return []
    la, lo = map(float, cell.split("|"))
    out = []
    for i in (-0.001, 0.0, 0.001):
        for j in (-0.001, 0.0, 0.001):
            out.append(f"{round(la+i,3)}|{round(lo+j,3)}")
    return out

def haversine_m(lat1, lon1, lat2, lon2):
    if None in (lat1,lon1,lat2,lon2): return 1e12
    R=6371000.0
    p1,p2=math.radians(lat1),math.radians(lat2)
    dphi=math.radians(lat2-lat1); dl=math.radians(lon2-lon1)
    a=math.sin(dphi/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def to_dec(x):
    """Convertit un nombre Python (int/float/str) en Decimal pour DynamoDB.
       Filtre NaN/Inf et None → renvoie None (à ne pas écrire dans l’item)."""
    if x is None:
        return None
    # éviter NaN / Inf
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    try:
        return Decimal(str(x))
    except (InvalidOperation, ValueError):
        return None


# ---------- S3 STREAMING ----------
def s3_csv_stream(bucket, key, delimiter=";"):
    obj = s3.get_object(Bucket=bucket, Key=key)
    stream = io.TextIOWrapper(obj["Body"], encoding="utf-8", newline="")
    reader = csv.DictReader(stream, delimiter=delimiter)
    for row in reader:
        yield row

# ---------- INDEX DES TRAVAUX ACTIFS ----------
def load_works_index(year, month, target_date):
    key = f"clean/works/{year}/{month:02d}/works_clean.csv"
    idx = defaultdict(list)
    try:
        for r in s3_csv_stream(CLEAN_BUCKET, key, delimiter=";"):
            d1 = parse_dt_or_date(r.get("start_ts") or "")
            d2 = parse_dt_or_date(r.get("end_ts") or "")
            sd = d1.date() if d1 else None
            ed = d2.date() if d2 else None
            # garder seulement les travaux actifs à cette date
            if sd and target_date < sd: 
                continue
            if ed and target_date > ed:
                continue
            try:
                lat = float(r["lat"]) if r["lat"] else None
                lon = float(r["lon"]) if r["lon"] else None
            except:
                lat, lon = None, None
            cell = grid1e3(lat, lon)
            if not cell:
                continue
            idx[cell].append({
                "work_id": r["work_id"],
                "impact": r.get("impact",""),
                "level":  r.get("level",""),
                "lat": lat, "lon": lon
            })
    except s3.exceptions.NoSuchKey:
        pass
    return idx

# ---------- AGRÉGATEUR ----------
class Aggregator:
    def __init__(self):
        self._agg = {}  # (work_id, segment_id, date, hour) -> state

    def add(self, work_id, segment_id, date_iso, hour, flow_per_min, traffic_status, impact, level, meters):
        key = (work_id, segment_id, date_iso, hour)
        a = self._agg.get(key)
        if not a:
            a = {
                "flows": [],
                "status_counts": defaultdict(int),
                "impact": impact,
                "level": level,
                "distance_min": meters
            }
            self._agg[key] = a
        if flow_per_min is not None: a["flows"].append(flow_per_min)
        if traffic_status: a["status_counts"][traffic_status] += 1
        if meters < a["distance_min"]: a["distance_min"] = meters

    def rows(self):
        out = []
        for (work_id, segment_id, date_iso, hour), a in self._agg.items():
            flows = a["flows"]
            avg_flow = sum(flows)/len(flows) if flows else None
            p90 = None
            if flows:
                s = sorted(flows)
                k = max(0, int(0.9*(len(s)-1)))
                p90 = s[k]
            status_mode = ""
            if a["status_counts"]:
                status_mode = max(a["status_counts"].items(), key=lambda x: x[1])[0]
            out.append({
                "work_id": work_id,
                "segment_id": segment_id,
                "date": date_iso,
                "hour": hour,
                "avg_flow_per_min": avg_flow if avg_flow is not None else "",
                "p90_flow_per_min": p90 if p90 is not None else "",
                "traffic_status": status_mode,
                "impact": a["impact"],
                "level": a["level"],
                "distance_m": round(a["distance_min"], 2) if a["distance_min"] < 1e11 else ""
            })
        return out

# ---------- NOUVELLES FONCTIONS D'ANALYSE AUTOMATIQUE ----------
def list_traffic_month_keys():
    keys = []
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=CLEAN_BUCKET, Prefix="clean/traffic/"):
        for it in page.get("Contents", []):
            k = it["Key"]
            if k.endswith("/traffic_clean.csv"):
                keys.append(k)
    return keys

def extract_dates_from_traffic_key(key_t):
    dates = set()
    try:
        for r in s3_csv_stream(CLEAN_BUCKET, key_t, delimiter=";"):
            ts = parse_dt_or_date(r.get("ts") or "")
            if not ts:
                continue
            d_iso, _ = day_hour_from_ts(ts)
            if d_iso:
                dates.add(d_iso)
    except s3.exceptions.NoSuchKey:
        pass
    return dates

# ---------- HANDLER PRINCIPAL ----------
def lambda_handler(event, context):
    traffic_keys = list_traffic_month_keys()
    if not traffic_keys:
        return {"ok": False, "msg": "Aucun fichier trafic trouvé"}

    all_rows = []

    for key_t in traffic_keys:
        try:
            parts = key_t.split("/")
            year = int(parts[2]); month = int(parts[3])
        except Exception:
            continue

        dates_in_file = extract_dates_from_traffic_key(key_t)
        if not dates_in_file:
            continue

        for date_iso in sorted(dates_in_file):
            target = datetime.fromisoformat(date_iso).date()
            works_idx = load_works_index(year, month, target)
            if not works_idx:
                continue

            agg = Aggregator()
            try:
                for r in s3_csv_stream(CLEAN_BUCKET, key_t, delimiter=";"):
                    ts = parse_dt_or_date(r.get("ts") or "")
                    d_iso, hour = day_hour_from_ts(ts)
                    if d_iso != date_iso:
                        continue
                    try:
                        lat = float(r["lat"]) if r["lat"] else None
                        lon = float(r["lon"]) if r["lon"] else None
                        flow = float(r["flow_per_min"]) if r["flow_per_min"] else None
                    except:
                        lat, lon, flow = None, None, None
                    cell = grid1e3(lat, lon)
                    if not cell:
                        continue

                    cand = []
                    for c in neighbors(cell):
                        lst = works_idx.get(c)
                        if lst:
                            cand.extend(lst)
                    if not cand:
                        continue

                    for w in cand:
                        meters = haversine_m(lat, lon, w["lat"], w["lon"])
                        if meters <= RADIUS_M:
                            agg.add(
                                work_id=w["work_id"],
                                segment_id=r["segment_id"],
                                date_iso=d_iso,
                                hour=hour,
                                flow_per_min=flow,
                                traffic_status=r.get("status",""),
                                impact=w["impact"],
                                level=w["level"],
                                meters=meters
                            )
            except s3.exceptions.NoSuchKey:
                continue

            rows = agg.rows()
            if rows:
                all_rows.extend(rows)

    if not all_rows:
        return {"ok": True, "msg": "Aucune correspondance trafic↔travaux sur les dates présentes."}

    # --- Écrire rapport global ---
    out_key = "reports/global/joined_all.csv"
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=[
        "work_id","segment_id","date","hour","avg_flow_per_min",
        "p90_flow_per_min","traffic_status","impact","level","distance_m"
    ], delimiter=";")
    w.writeheader()
    for r in all_rows:
        w.writerow(r)
    s3.put_object(Bucket=REPORT_BUCKET, Key=out_key,
                  Body=buf.getvalue().encode("utf-8"),
                  ContentType="text/csv")

    # --- Charger DynamoDB ---
    with table.batch_writer(overwrite_by_pkeys=["pk","sk"]) as bw:
        for r in all_rows:
            item = {
                "pk": f"WORK#{r['work_id']}#DATE#{r['date']}",
                "sk": f"SEG#{r['segment_id']}#HOUR#{int(r['hour'])}",
                "metric_type": "work_impact",
                "work_id": r["work_id"],
                "segment_id": r["segment_id"],
                "date": r["date"],
                "hour": int(r["hour"]),  # int natif OK
                "traffic_status": r["traffic_status"],
                "impact": r["impact"],
                "level": r["level"]
            }

            # Convertir les numériques en Decimal
            avg = r.get("avg_flow_per_min")
            p90 = r.get("p90_flow_per_min")
            dist = r.get("distance_m")

            avg_d = to_dec(avg if avg != "" else None)
            p90_d = to_dec(p90 if p90 != "" else None)
            dist_d = to_dec(dist if dist != "" else None)

            if avg_d is not None:  item["avg_flow_per_min"] = avg_d
            if p90_d is not None:  item["p90_flow_per_min"] = p90_d
            if dist_d is not None: item["distance_m"]       = dist_d

            # (optionnel) retirer toute clé avec None au cas où
            item = {k: v for k, v in item.items() if v is not None}

            bw.put_item(Item=item)

    return {
        "ok": True,
        "report_s3": f"s3://{REPORT_BUCKET}/{out_key}",
        "rows": len(all_rows)
    }
