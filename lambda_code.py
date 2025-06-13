import boto3
import PyPDF2
import io
import json
import urllib.parse
import os

from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

def extract_extension(key):
    try:
        pos = key.rfind('.')
        return key[pos:] if pos != -1 else ''
    except Exception as e:
        print(f'Error al extraer la extensión: {e}')
        return None

def get_queue_url_from_arn(sqs, queue_arn):
    queue_name = queue_arn.split(':')[-1]
    response = sqs.get_queue_url(QueueName=queue_name)
    return response['QueueUrl']

def delete_sqs_message(sqs, record, queue_arn):
    try:
        queue_url = get_queue_url_from_arn(sqs, queue_arn)
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=record['receiptHandle'])  # Eliminar el mensaje de la cola
    except Exception as e:
        print(f'Error al eliminar el mensaje de la cola: {e}')

def process_PDF(file_content):
    try:
        pdf_reader = PyPDF2.PdfReader(io.BytesIO(file_content))
        text_list = []
        
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text = page.extract_text()
            if text:
                text_list.append(text.strip())

        return ' '.join('. '.join(text_list).split())
    
    except Exception as e:
        print(f'Error procesando el texto: {e}')
        return None

def process_TXT(file_content):
    try:
        text = file_content.decode('utf-8', errors='ignore')
        cleaned_text = '. '.join(line.strip() for line in text.splitlines() if line.strip())
        return cleaned_text

    except Exception as e:
        print(f'Error procesando el texto: {e}')
        return None

def opensearch_index(title, text, db_index):
    try:
        doc_title = os.path.splitext(os.path.basename(title))[0]
        document = {
            "index": db_index,
            "title": doc_title,
            "content": text
        }

        region = 'eu-west-1'
        service = 'es'
        host = 'change_me'
        index_name = 'change_me'

        # Obtener credenciales temporales de la IAM role de la Lambda
        credentials = boto3.Session().get_credentials()
        awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

        # Cliente de OpenSearch con firma IAM
        client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=awsauth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        response = client.index(index=index_name, body=document)
        print(response)
    except Exception as e:
        print(f'Error al indexar en OpenSearch: {e}')

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    sqs = boto3.client('sqs')
    queue_arn = event['Records'][0]['eventSourceARN']
    
    try:
        print(event['Records'])
        
        record = event['Records'][0]
        receive_count = int(record['attributes']['ApproximateReceiveCount'])

        s3_event = json.loads(record['body'])

        if 'Records' in s3_event and len(s3_event['Records']) == 1:
            s3_record = s3_event['Records'][0]
            if 's3' in s3_record:
                bucket_name = s3_record['s3']['bucket']['name']
                object_key = urllib.parse.unquote_plus(s3_record['s3']['object']['key'])

                try:
                    response = s3.get_object(Bucket=bucket_name, Key=object_key)
                    file_content = response['Body'].read()
                    db_index = response['Metadata'].get('db-index', 'default-index')
                    # Evitar reprocesar mensajes más de 2 veces
                    if receive_count > 2:
                        opensearch_index(object_key, "", db_index)
                        delete_sqs_message(sqs, record, queue_arn)
                        return
                except Exception as e:
                    print(f"Error al obtener el archivo S3: {e}")
                    delete_sqs_message(sqs, record, queue_arn)
                    return

                ext = extract_extension(object_key)
                text = None

                if ext == '.pdf':
                    text = process_PDF(file_content)
                elif ext == '.txt':
                    text = process_TXT(file_content)
                else:
                    # Extensión no reconocida → indexar sin contenido
                    try:
                        opensearch_index(object_key, '', db_index)
                    except Exception as e:
                        print(f"Error al indexar en OpenSearch: {e}")
                    delete_sqs_message(sqs, record, queue_arn)
                    return

                # Si no hay texto, indexar igual (como con extensión desconocida)
                if not text:
                    print(f"No se extrajo texto del archivo {object_key}. Se indexará sin contenido.")
                    try:
                        opensearch_index(object_key, '', db_index)
                    except Exception as e:
                        print(f"Error al indexar en OpenSearch: {e}")
                    delete_sqs_message(sqs, record, queue_arn)
                    return

                # Si hay texto, indexar normalmente
                try:
                    opensearch_index(object_key, text, db_index)
                except Exception as e:
                    print(f"Error al indexar en OpenSearch: {e}")

    except Exception as e:
        print(f"Error procesando el evento: {e}")

    # Eliminar el mensaje de la cola SQS después de procesarlo
    delete_sqs_message(sqs, record, queue_arn)