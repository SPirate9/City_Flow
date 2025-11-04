import json
import urllib.request
import boto3
from datetime import datetime

def lambda_handler(event, context):
    # Récupérer toutes les données avec pagination
    all_data = []
    offset = 0
    limit = 100
    
    while True:
        url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/chantiers-perturbants/records?limit={limit}&offset={offset}"
        
        with urllib.request.urlopen(url) as response:
            results = json.loads(response.read().decode("utf-8")).get("results", [])
        
        if not results:
            break
        
        all_data.extend(results)
        offset += limit
    
    # Récupérer toutes les colonnes disponibles
    if all_data:
        headers = list(all_data[0].keys())
        csv_data = ";".join(headers) + "\n"
        
        for d in all_data:
            row = []
            for h in headers:
                value = str(d.get(h, '')).replace(';', ',').replace('\n', ' ')
                row.append(value)
            csv_data += ";".join(row) + "\n"
    else:
        csv_data = ""
    
    # Envoi vers S3
    s3 = boto3.client("s3")
    bucket = "cityflow-raw-paris"
    key = f"api/chantier_{datetime.now().strftime('%Y%m%d')}.csv"
    
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_data.encode("utf-8")
    )
    
    return {
        "status": "success",
        "s3_file": f"s3://{bucket}/{key}",
        "total_records": len(all_data)
    }
