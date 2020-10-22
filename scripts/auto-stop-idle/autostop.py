import requests
from datetime import datetime
import getopt, sys
import urllib3
import boto3
import json
import time
from botocore.exceptions import ClientError
from urllib.parse import urljoin
from urllib.parse import urlencode
import urllib.request as urlrequest
import sys

def mail_send(RECIPIENT,BODY_TEXT):
    AWS_REGION = region
    SUBJECT = "sagemaker monitoring"
    SENDER = sender_adress    
    CHARSET = "UTF-8"
    client = boto3.client('ses',region_name=AWS_REGION)

    try:
        #Provide the contents of the email.
        response = client.send_email(
            Destination={
                'ToAddresses': [
                    RECIPIENT,
                ],
            },
            Message={
                'Body': {
                    'Text': {
                        'Charset': CHARSET,
                        'Data': BODY_TEXT,
                    },
                },
                'Subject': {
                    'Charset': CHARSET,
                    'Data': SUBJECT,
                },
            },
            Source=SENDER
        )
    except ClientError as e:
        print(e.response['Error']['Message'])
    else:
        print("Email sent! Message ID:"),
        print(response['MessageId'])
        
class Slack():

    def __init__(self, url=""):
        self.url = url
        self.opener = urlrequest.build_opener(urlrequest.HTTPHandler())

    def notify(self, **kwargs):
        """
        Send message to slack API
        """
        return self.send(kwargs)

    def send(self, payload):

        payload_json = json.dumps(payload)
        data = urlencode({"payload": payload_json})
        req = urlrequest.Request(self.url)
        response = self.opener.open(req, data.encode('utf-8')).read()
        return response.decode('utf-8')

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Usage
usageInfo = """Usage:
This scripts checks if a notebook is idle for X seconds if it does, it'll stop the notebook:
python autostop.py --time <time_in_seconds> [--port <jupyter_port>] [--ignore-connections]
Type "python autostop.py -h" for available options.
"""
# Help info
helpInfo = """-t, --time
    Auto stop time in seconds
-p, --port
    jupyter port
-c --ignore-connections
    Stop notebook once idle, ignore connected users
-s --slack slack chanel
-m --mail mail to
-r, --region   aws region  
-f, --sender mail of sender
-h, --help
    Help information
"""
idle = True
port = '8443'

ignore_connections = False
try:
    opts, args = getopt.getopt(sys.argv[1:], "ht:p:s:m:r:f:c", ["help","time=","port=","slack=",
                                                                "mail=","region=","sender=","ignore-connections"])
    if len(opts) == 0:
        raise getopt.GetoptError("No input parameters!")
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            print(helpInfo)
            exit(0)
        if opt in ("-t", "--time"):
            time = int(arg)
        if opt in ("-p", "--port"):
            port = str(arg)
        if opt in ("-c", "--ignore-connections"):
            ignore_connections = True
        if opt in ("-s","--slack"):
            slack_adress = str(arg)
            slack = Slack(url=slack_adress)
        if opt in ("-m", "--mail"):
            mail_adress = str(arg)    
        if opt in ("-r", "--region"):
            region = str(arg)     
        if opt in ("-f", "--sender"):
            sender_adress = str(arg)        
except getopt.GetoptError:
    print(usageInfo)
    exit(1)

missingConfiguration = False
if not time:
    print("Missing '-t' or '--time'")
    missingConfiguration = True
if missingConfiguration:
    exit(2)


def is_idle(last_activity):
    last_activity = datetime.strptime(last_activity,"%Y-%m-%dT%H:%M:%S.%fz")
    if (datetime.now() - last_activity).total_seconds()/60 > time:
        print('Notebook is idle. Last activity time = ', last_activity)
        return True
    else:
        print('Notebook is not idle. Last activity time = ', last_activity)
        return False


def get_notebook_name():
    log_path = '/opt/ml/metadata/resource-metadata.json'
    with open(log_path, 'r') as logs:
        _logs = json.load(logs)
    return _logs['ResourceName']

response = requests.get('https://localhost:'+port+'/api/sessions', verify=False)
data = response.json()
if len(data) > 0:
    for notebook in data:
        if notebook['kernel']['execution_state'] == 'idle':
            if not ignore_connections:
                if notebook['kernel']['connections'] == 0:
                    if not is_idle(notebook['kernel']['last_activity']):
                        idle = False
                else:
                    idle = False
            else:
                if not is_idle(notebook['kernel']['last_activity']):
                    idle = False
        else:
            print('Notebook is not idle:', notebook['kernel']['execution_state'])
            idle = False
else:
    client = boto3.client('sagemaker')
    uptime = client.describe_notebook_instance(
        NotebookInstanceName=get_notebook_name()
    )['LastModifiedTime']
    if not is_idle(uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz")):
        idle = False

if idle:
    print('Closing idle notebook')
    client = boto3.client('sagemaker')
    client.stop_notebook_instance(
        NotebookInstanceName=get_notebook_name()
    )
    msg = ("sagemaker shuting idle notebook !! \n"+
             "notebook name = "+get_notebook_name()+
             " \n time from last activity  = "+str((datetime.now() -datetime.strptime(uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz"),"%Y-%m-%dT%H:%M:%S.%fz")).total_seconds()/60 )+" minutes")
    mail_send(mail_adress,msg)
    #slack.notify(text=msg)
else:
    msg = ("sagemaker notebook monitoring  \n"+
             "notebook name = "+get_notebook_name()+
             " \n time from last activity  = "+ str((datetime.now() -datetime.strptime(uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz"),"%Y-%m-%dT%H:%M:%S.%fz")).total_seconds()/60) +" minutes")
    if (datetime.now() -datetime.strptime(uptime.strftime("%Y-%m-%dT%H:%M:%S.%fz"),"%Y-%m-%dT%H:%M:%S.%fz")).total_seconds()/60 > 120 :
        mail_send(mail_adress,msg) 
        #slack.notify(text=msg)
    print('Notebook not idle. Pass.')
