from collections import defaultdict
import boto3, json, decimal

ddb = boto3.resource("dynamodb")
table = ddb.Table("cityflow_metrics")

def decimal_default(obj):
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    response = table.scan(Limit=100)  # Limite pour test
    items = response.get("Items", [])

    agg = defaultdict(lambda: defaultdict(list))
    for it in items:
        seg = it.get("segment_id")
        hour = it.get("hour")
        flow = it.get("avg_flow_per_min")
        if seg is None or hour is None or flow is None:
            continue
        agg[seg][int(hour)].append(float(flow))

    result = []
    for seg, hours in agg.items():
        for h, flows in hours.items():
            result.append({
                "segment_id": seg,
                "hour": h,
                "avg_flow_per_min": sum(flows)/len(flows)
            })

    return {"statusCode": 200, "body": json.dumps(result, default=decimal_default)}
