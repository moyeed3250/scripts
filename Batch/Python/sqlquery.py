import logging
import boto3
from botocore.exceptions import ClientError
import pymssql

connection={
        'host': 'database-1.c1yjaupfzfk9.us-east-1.rds.amazonaws.com',
        'username': 'admin',
        'password': 'test1234',
        'db': 'batchDb' 
 }
con=pymssql.connect(connection['host'],connection['username'],connection['password'],connection['db'])
cursor = con.cursor()

sqlpull='SELECT * FROM guest.testDb'
cursor.execute(sqlpull)
result=cursor.fetchall()
with open("test.csv","wt") as out_file:
    for name,email,website,image,_ in result:
        out_file.write(f'{name},{email},{website},{image}\n')
               
con.close()

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # Upload the file
    s3_client = boto3.client('s3')
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True
upload_status = upload_file('test.csv', 'moyeedbatches')
print (upload_status)



