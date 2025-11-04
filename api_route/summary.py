import boto3, json
from collections import defaultdict
import decimal

table = boto3.resource("dynamodb").Table("cityflow_metrics")

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    try:
        response = table.scan(Limit=100)
        items = response.get("Items", [])

        # Regroupe par segment_id pour exemple
        agg = defaultdict(lambda: {"total_flow": 0, "count": 0})
        for it in items:
            seg = it.get("segment_id")
            flow = it.get("avg_flow_per_min")
            if seg is None or flow is None:
                continue
            agg[seg]["total_flow"] += float(flow)
            agg[seg]["count"] += 1

        result = []
        for seg, v in agg.items():
            avg = v["total_flow"]/v["count"] if v["count"] else 0
            result.append({
                "segment_id": seg,
                "total_flow": v["total_flow"],
                "avg_flow": avg,
                "records_count": v["count"]
            })

        return {"statusCode": 200, "body": json.dumps(result, default=decimal_default)}

    except Exception as e:
        return {"statusCode": 500, "body": str(e)}
