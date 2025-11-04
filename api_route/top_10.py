from collections import defaultdict
import boto3, json, decimal

ddb = boto3.resource("dynamodb")
table = ddb.Table("cityflow_metrics")

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    response = table.scan(Limit=100)
    items = response.get("Items", [])

    agg = defaultdict(float)
    for it in items:
        seg = it.get("segment_id")
        flow = it.get("avg_flow_per_min")
        if seg is None or flow is None:
            continue
        agg[seg] = max(agg[seg], float(flow))

    top10 = sorted([{"segment_id": k, "max_flow": v} for k,v in agg.items()],
                   key=lambda x: x["max_flow"], reverse=True)[:10]

    return {"statusCode": 200, "body": json.dumps(top10, default=decimal_default)}
