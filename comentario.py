import boto3
import uuid
import os
import json
from datetime import datetime

def lambda_handler(event, context):
    # Entrada (json)
    print(event)
    tenant_id = event['body']['tenant_id']
    texto = event['body']['texto']
    nombre_tabla = os.environ["TABLE_NAME"]
    nombre_bucket = os.environ["BUCKET_NAME"]
    
    # Proceso
    uuidv1 = str(uuid.uuid1())
    comentario = {
        'tenant_id': tenant_id,
        'uuid': uuidv1,
        'detalle': {
          'texto': texto
        }
    }
    
    # Guardar en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(nombre_tabla)
    response = table.put_item(Item=comentario)
    
    # Guardar en S3 (Ingesta Push)
    s3 = boto3.client('s3')
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    s3_key = f"{tenant_id}/{timestamp}_{uuidv1}.json"
    
    s3.put_object(
        Bucket=nombre_bucket,
        Key=s3_key,
        Body=json.dumps(comentario, indent=2),
        ContentType='application/json'
    )
    
    # Salida (json)
    print(comentario)
    print(f"Comentario guardado en S3: s3://{nombre_bucket}/{s3_key}")
    
    return {
        'statusCode': 200,
        'comentario': comentario,
        'response': response,
        's3_location': f"s3://{nombre_bucket}/{s3_key}"
    }
