import base64
import boto3
from botocore.exceptions import ClientError
import os
import time
import datetime
import logging  
import io
import subprocess



aws_access_key_id =''
aws_secret_access_key = ''
region_name = 'us-east-1'
request_queue_url = ''
response_queue_url = ''
endpoint_url = ''
s3_input_bucket = ""
s3_output_bucket = ""
# s3 = boto3.resource(
#     service_name='s3',
#     region_name=region_name,
#     aws_access_key_id=aws_access_key_id,
#     aws_secret_access_key=aws_secret_access_key
#     )

sqs = boto3.client('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)
s3_client = boto3.client('s3', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)



def poll_for_requests() :
    print("Polling for messages:")

    try:
        response = sqs.receive_message(
            QueueUrl=request_queue_url,
                AttributeNames=[
                'SentTimestamp'
                ],
                MaxNumberOfMessages=1,
                MessageAttributeNames=[
                'All'
                ],
                VisibilityTimeout=30,
                WaitTimeSeconds = 20
            )

    except Exception as e:
        print(str(e))
        return "Something went wrong"


    if 'Messages' in response :
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        resp = response['Messages']

        print(resp)
        delete_message_from_request_queue(receipt_handle)
        return resp
    else :
        time.sleep(1)
        return poll_for_requests()

def delete_message_from_request_queue(receipt_handle) :
    sqs.delete_message(
        QueueUrl = request_queue_url,
        ReceiptHandle = receipt_handle
    )

def decode_message(file_name, msg) :
    decode_it=open(file_name,'wb')
    decode_it.write(base64.b64decode((msg)))
    decode_it.close()

def send_message_in_response_queue(file_name, msg) :
    endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
    resp = sqs.send_message(
    QueueUrl = response_queue_url,
        MessageBody=(
        file_name + " " + msg
        )
    
    )
    
def upload_to_s3_input_bucket(file_obj, bucket, object_name) :
    try:
        response = s3_client.upload_fileobj(file_obj, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def upload_to_s3_output_bucket( image_name, predicted_result) :
    content = (image_name, predicted_result)
    content = ' '.join(str(x) for x in content)
    # s3_resource.Object(s3_output_bucket, image_name).put(Body=content)
    res = s3_client.put_object(
        Bucket = s3_output_bucket,
        Key = image_name,
        Body = predicted_result
    )

def init() :

    val = poll_for_requests()
    
    if(val == None or len(val) == 0):
        print('Some error occurred. No requests found')
        return
    
    message = val[0]
    file_name , encoded_message=message['Body'].split()
    file_name_withput_extension = file_name
    file_name = file_name + ".jpg"
    print('file name : ' + file_name)

    msg_value = bytes(encoded_message, 'ascii')
    qp = base64.b64decode(msg_value)
    print(qp)
    with open(file_name, "wb") as file:
        file.write(qp)

    with open(file_name, 'rb') as f:
        if upload_to_s3_input_bucket(f, s3_input_bucket, file_name):
            print("uploaded image to S3 bucket")

    command = ['python3', 'face_recognition.py', file_name]
    cwd = os.getcwd()
    os.chdir('/home/ec2-user/cc_project_1/model')
    result = subprocess.run(command, capture_output=True, text=True)
    os.chdir(cwd)
    
    # stdout = os.popen(f'python3 ./face_recognition.py "{file_name}"')
    # result = stdout.read().strip()
    output = result.stdout[:-1]
    print("result of stdout" + result.stdout[:-1])
    print("result " + output)
    
    logging.info('result : ' + output + ' '+file_name)
    print("result " + output + ' '+file_name)

    with open(file_name, 'rb') as f:
        upload_to_s3_output_bucket( file_name_withput_extension, output)
        send_message_in_response_queue(file_name_withput_extension, output)

    print(result)
    
    

logging.info('Timestamp : ' + str(datetime.datetime.now()))
while True :
    init()
