import boto3
import uuid
import os
import json

def lambda_handler(event, context):
    try:
        # --- ENTRADA ---
        print(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "evento_recibido": event
            }
        }))
        
        # Asegurar que el body sea un dict (API Gateway lo manda como string)
        body = event.get('body', {})
        if isinstance(body, str):
            body = json.loads(body)
        
        tenant_id = body['tenant_id']
        pelicula_datos = body['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # --- PROCESO ---
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }

        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # --- LOG INFO ---
        print(json.dumps({
            "tipo": "INFO",
            "log_datos": {
                "mensaje": "Película insertada correctamente",
                "tenant_id": tenant_id,
                "uuid": uuidv4,
                "pelicula_datos": pelicula_datos
            }
        }))

        # --- SALIDA ---
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }

    except Exception as e:
        # --- LOG ERROR ---
        print(json.dumps({
            "tipo": "ERROR",
            "log_datos": {
                "mensaje": "Error al insertar película",
                "error": str(e),
                "event": event
            }
        }))

        # --- SALIDA DE ERROR ---
        return {
            'statusCode': 500,
            'error': str(e)
        }
