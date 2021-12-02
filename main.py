
import builtins
import hashlib
import json

from flask import Flask
from flask.globals import request
from google.cloud import bigquery
from google.cloud import secretmanager
import datetime
from google.cloud import storage
from google.cloud.storage import blob

app = Flask(__name__)


def process_jenkins_event(request):

    # autheticate the curl request
    headers = dict(request.headers)
    #source = get_source(headers)    
    source = 'jenkins'
    #if source not in AUTHORIZED_SOURCES:
     #   raise Exception(f"Source not authorized: {source}")

    #auth_source = AUTHORIZED_SOURCES[source]
    #signature_sources = {**request.headers, **request.args}
    #signature = signature_sources.get(auth_source.signature, None)
    body = request.data

    # Verify the signature
    #verify_signature = auth_source.verification
    #if not verify_signature(signature, body):
    #    raise Exception("Unverified Signature")

    # process the event
    envelope = request.get_json()
    
    #source = "jenkins"
    body = request.data
    e_id = envelope.get("id")
    epoch = envelope.get("timestamp")/1000
    time_created = datetime.datetime.utcfromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')
    msg_id = envelope.get("number")
    actions = envelope.get("actions")
    #main_commit = actions[3].get("lastBuiltRevision").get("SHA1")
    main_commit = None
    #main_commit = actions[2].get("lastBuiltRevision").get("SHA1")
    i = 0
    #commit = actions[0].get("lastBuiltRevision").get("SHA1")
    while actions:
         commit =  actions[i].get("lastBuiltRevision")
         if commit is not None:
             main_commit = actions[i].get("lastBuiltRevision").get("SHA1")
             break
         else:
             i = i + 1

    metadata = {
        "result": envelope.get("result"), 
        "url": envelope.get("url"),
        "previousBuild": envelope.get("previousBuild"),
        "pipeline.vcs.revision": main_commit
    }
    msg = envelope.get("fullDisplayName")
    signature = create_unique_id(msg)
    build_event = {
        "event_type": 'build',
        "id": e_id,
        "metadata": json.dumps(metadata),
        #"metadata": headers,
        "time_created": time_created,
        "signature": signature,
        "msg_id": msg_id,
        "source": source,
    }  


    # Publish to Pub/Sub
   # publish_to_pubsub(source, body, headers)
    #insert_row_into_bigquery(build_event)
    
    # save to local
    #with open('rawdata.json', 'a') as f:
    #    json.dump(build_event, f)
    #    f.write("\n")

    #save to cloud storage
    insert_event_into_storage(build_event)
    return build_event

def insert_event_into_storage(event):
    client = storage.Client(project='FourKeys')
    bucket = client.get_bucket('test-bucket-for-raw-data')
    blob = bucket.blob('t.json')
    with open('rawdata.json', 'rb') as f:
        blob.upload_from_file(f)

def insert_row_into_bigquery(event):
    if not event:
        raise Exception("No data to insert")

    # Set up bigquery instance
    client = bigquery.Client()
    dataset_id = "four_keys"
    table_id = "events_raw"

    if is_unique(client, event["signature"]):
        table_ref = client.dataset(dataset_id).table(table_id)
        table = client.get_table(table_ref)

        # Insert row
        row_to_insert = [
            (
                event["event_type"],
                event["id"],
                event["metadata"],
                event["time_created"],
                event["signature"],
                event["msg_id"],
                event["source"],
            )
        ]
        bq_errors = client.insert_rows(table, row_to_insert)

        # If errors, log to Stackdriver
        if bq_errors:
            entry = {
                "severity": "WARNING",
                "msg": "Row not inserted.",
                "errors": bq_errors,
                "row": row_to_insert,
            }
            print(json.dumps(entry))

def show_the_login_form():
    return 'showing login form'

def create_unique_id(msg):
    hashed = hashlib.sha1(bytes(json.dumps(msg), "utf-8"))
    return hashed.hexdigest()

def is_unique(client, signature):
    sql = "SELECT signature FROM four_keys.events_raw WHERE signature = '%s'"
    query_job = client.query(sql % signature)
    results = query_job.result()
    return not results.total_rows

def get_source(headers):
    """
    Gets the source from the User-Agent header
    """
    if "X-Gitlab-Event" in headers:
        return "gitlab"

    if "tekton" in headers.get("Ce-Type", ""):
        return "tekton"

    if "GitHub-Hookshot" in headers.get("User-Agent", ""):
        return "github"

    if "curl" in headers.get("User-Agent", ""):
        return "jenkins"        

    return headers.get("User-Agent")
    
class EventSource(object):
    """
    A source of event data being delivered to the webhook
    """

    def __init__(self, signature_header, verification_func):
        self.signature = signature_header
        self.verification = verification_func


def simple_token_verification(token, body):
    """
    Verifies that the token received from the event is accurate
    """
    if not token:
        raise Exception("Token is empty")
    #secret = get_secret("fourkeys-003116", "event-handler", "1")
    secret = "fa26cba9daf0dc179da0b88f44fb4b650f1a15df"
    return secret.decode() == token

def get_secret(project_name, secret_name, version_num):
    """
    Returns secret payload from Cloud Secret Manager
    """
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = client.secret_version_path(
            project_name, secret_name, version_num
        )
        secret = client.access_secret_version(name)
        return secret.payload.data
    except Exception as e:
        print(e)

AUTHORIZED_SOURCES = {
    "github": EventSource(
        "X-Hub-Signature", simple_token_verification
        ),
    "gitlab": EventSource(
        "X-Gitlab-Token", simple_token_verification
        ),
    "tekton": EventSource(
        "tekton-secret", simple_token_verification
        ),
    "jenkins": EventSource(
        "jenkins-secret", simple_token_verification
        )    
}

@app.route('/', methods=['POST'])
def index():
    return process_jenkins_event(request)  
