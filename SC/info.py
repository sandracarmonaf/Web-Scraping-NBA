
#1. Guardar en Amazon S3 (Almacenamiento de archivos)
#Si quieres guardar los DataFrames como archivos CSV o Parquet en S3, usa boto3:

import boto3
import pandas as pd
from io import StringIO  # Para guardar en memoria como CSV

# Configurar AWS (asegúrate de tener credenciales en ~/.aws/credentials o config)
s3_client = boto3.client('s3', region_name='us-east-1')

# Convertir DataFrame a CSV en memoria
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})  # Ejemplo de DataFrame
csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

# Subir a S3
s3_client.put_object(
    Bucket='mi-bucket',
    Key='datos/basquet.csv',
    Body=csv_buffer.getvalue()
)

print("Archivo subido a S3 exitosamente")

# 2. Guardar en Amazon RDS (Base de Datos Relacional)
pip install sqlalchemy psycopg2-binary  # Para PostgreSQL
pip install pymysql  # Para MySQL

#Ejemplo para PostgreSQL en RDS:
from sqlalchemy import create_engine

# Configurar conexión a RDS PostgreSQL
engine = create_engine("postgresql://usuario:contraseña@rds-endpoint.amazonaws.com:5432/nombre_db")

# Guardar DataFrame en la base de datos
df.to_sql('tabla_basquet', engine, if_exists='replace', index=False)

print("Datos subidos a RDS exitosamente")

# 3. Guardar en DynamoDB (Base NoSQL)
#Si los datos son más tipo JSON o NoSQL, DynamoDB es una opción:
pip install boto3

dynamodb = boto3.resource('dynamodb')
tabla = dynamodb.Table('tabla_basquet')

# Insertar datos fila por fila
for _, row in df.iterrows():
    tabla.put_item(Item=row.to_dict())

print("Datos subidos a DynamoDB exitosamente")