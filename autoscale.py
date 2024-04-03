

import threading
import time


import boto3



aws_access_key_id =''
aws_secret_access_key = ''
region_name = 'us-east-1'
endpoint_url = ''
request_queue_url = ''
response_queue_url = ''
sqs = boto3.client('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)

ami_id=""
instance_type = 't2.micro'  
key_name = 'General_us_east_1'  
user_data = '''#!/bin/bash
cd /home/ubuntu
sudo pip3 install boto3
sudo -u ubuntu python3 /home/ubuntu/App_Teir.py'''


instance_name = 'app-tier-instance-'

ec2 = boto3.client('ec2', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)


class RequestManager:
    def __init__(self):
        self.num_requests = 0
        self.instance_ids = []
        self.lock = threading.Lock()

    def increase_requests(self):
        with self.lock:
            self.num_requests += 1
            print(f"Request #{self.num_requests} received.")
            if(self.num_requests <=19):
                self.start_ec2_instances(self.num_requests)

    def requests_fulfilled(self):
        try:
            with self.lock:
                self.num_requests -= 1
                if(self.num_requests ==0):
                    self.terminate_ec2_instances()
            print("returning from requests_fulfilled")
            return True
        except Exception as e:
            print(str(e))
            return False

    def reset_requests(self):
        with self.lock:
            self.num_requests = 0

    def start_ec2_instances(self, number):
        print("Starting EC2 instances..." + instance_name + str(number))
        # Add logic to start EC2 instances here
        
        response = ec2.run_instances(
            ImageId = ami_id,       # Specify your AMI ID
            InstanceType='t2.micro',      # Specify instance type
            MinCount=1,                    # Launch only one instance
            MaxCount=1,                    # Launch only one instance
            KeyName= key_name,        # Specify your key pair name
            UserData= user_data,            # Specify your user data
            TagSpecifications=[
                {
                    'ResourceType': 'instance',
                    'Tags': [
                        {
                            'Key': 'Name',
                            'Value': instance_name + str(number)
                        },
                    ]
                },
            ]
        )
        
        for instance in response['Instances']:
            self.instance_ids.append(instance['InstanceId'])

    def terminate_ec2_instances(self):
        print()
        def delayed_terminate():
            time.sleep(30)
            try:
                print('before termination' + str(self.instance_ids))
                if self.check_if_queue_is_empty():
                    response = ec2.terminate_instances(InstanceIds=self.instance_ids)
                    if 'TerminatingInstances' in response:
                        for instance in response['TerminatingInstances']:
                            print(f"Instance {instance['InstanceId']} is being terminated.")
                            self.instance_ids.remove(instance['InstanceId'])
                    else:
                        print("No instances terminated.")
                print('after termination' + str(self.instance_ids))
                self.reset_requests()
            except Exception as e:
                print(str(e))

        # Create a new thread to execute the delayed_terminate function
        thread = threading.Thread(target=delayed_terminate)
        thread.start()
        return True

    def check_if_queue_is_empty(self):
        req_response = sqs.get_queue_attributes(
            QueueUrl=request_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        resp_response = sqs.get_queue_attributes(
            QueueUrl=response_queue_url,
            AttributeNames=['ApproximateNumberOfMessages']
        )
        
        req  = False
        resp = False
        if 'ApproximateNumberOfMessages' in req_response['Attributes']:
            num_messages = int(req_response['Attributes']['ApproximateNumberOfMessages'])
            if(num_messages == 0):
                req = True
        else:
        # Queue attribute not found, handle accordingly
            req = False  # Assume queue is not empty if attribute not found
        
        
        if 'ApproximateNumberOfMessages' in resp_response['Attributes']:
            num_messages = int(resp_response['Attributes']['ApproximateNumberOfMessages'])
            if(num_messages == 0):
                resp = True
        else:
        # Queue attribute not found, handle accordingly
            resp = False  # Assume queue is not empty if attribute not found
        
        return req and resp
            


# # Example usage:
# if __name__ == "__main__":
#     request_manager = RequestManager()

#     # Simulate increase in requests
#     request_manager.start_ec2_instances(1)
#     print(request_manager.instance_ids)