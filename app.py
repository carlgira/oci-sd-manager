from flask import Flask, render_template, request, jsonify
import requests
import os
import subprocess
import threading
import pandas as pd
from flask_cors import CORS
import random
import oci
import re
import time
from oci.object_storage import ObjectStorageClient
import sched

WORK_DIR = os.environ['install_dir']
SEVERS_FILE= 'data/servers.csv'
WORK_REQUEST_FILE = 'data/work_requests.csv'
PROMPTS_FILE = 'data/prompts.csv'

servers = pd.read_csv(SEVERS_FILE)
work_requests = pd.read_csv(WORK_REQUEST_FILE)
prompts = pd.read_csv(PROMPTS_FILE)

flask = Flask(__name__)
cors = CORS(flask)
OIC_URL = os.environ['OIC_URL']
cors_origins = [OIC_URL, 'http://localhost']

object_storage_client = ObjectStorageClient(config=oci.config.from_file('~/.oci/config'))

@flask.route('/work_requests', methods=['PUT', 'GET', 'POST'])
def work_requests_api():
    global work_requests
    
    if request.method == 'PUT':
        content = request.get_json()
        mail = content['mail']
        status = content['status']
        
        update_status_work_request(mail, status)
    
        return jsonify({'status': 'success', 'message': 'Work request updated'})

    if request.method == 'GET':
        mail = request.args.get('mail')
        if mail is None:
            return jsonify(work_requests.to_dict(orient='records'))
        
        return jsonify(work_requests.loc[work_requests['mail'] == mail].to_dict(orient='records'))
        
    if request.method == 'POST':
        content = request.get_json()
        mail = content['mail']
        server = content['server']
        work_requests = work_requests.append({'mail': mail, 'server': server, 'tag': None, 'session' : None, 'status': 'created'}, ignore_index=True)
        work_requests.to_csv(WORK_REQUEST_FILE, index=False)
        
        return jsonify({'status': 'success', 'message': 'Work request created'})
    
    return jsonify({'status': 'error', 'message': 'Invalid request'})


@flask.route('/servers', methods=['GET'])
def servers_api():
    global servers
    if request.method == 'GET':
        return jsonify(servers.to_dict(orient='records'))
    return jsonify({'status': 'error', 'message': 'Invalid request'})


@flask.route('/submit', methods=['POST'])
def submit():
    global work_requests
    if request.method == 'POST':
        
        content = request.get_json()
        mail = content['mail']
        server = content['server']
        tag = content['tag']
        
        images = content['images']
        session = ''.join(random.choice('abcdefghijklmnopqrtsvwyz') for i in range(10))
        
        work_requests.loc[work_requests['mail'] == mail, 'session'] = session
        work_requests.loc[work_requests['mail'] == mail, 'tag'] = tag
        work_requests.to_csv(WORK_REQUEST_FILE, index=False)
        
        SESSION_DIR = 'sessions/' + session
        os.mkdir(SESSION_DIR)
        
        if not is_training_running(mail):
            
            if images is None or len(images) == 0:
                return jsonify(message='No file uploaded', category="error", status=500)
            
            for i, img_url in enumerate(images):
                url_parts = extract_fields_from_url(img_url)
                namespace = url_parts['namespace']
                bucket = url_parts['bucket']
                folder = url_parts['mail']
                filename = url_parts['filename']
                extension = filename.split('.')[-1]
                
                img_content = object_storage_client.get_object(namespace, bucket, folder + '/' + filename).data.content
                # save image to disk
                
                with open(SESSION_DIR + '/' + str(i) + '.' + extension, 'wb') as f:
                    f.write(img_content)
                
            zip_file = SESSION_DIR + '/images.zip'
            subprocess.getoutput("zip -j {ZIP_FILE} {ZIP_FILES}".format(ZIP_FILE=zip_file, ZIP_FILES=SESSION_DIR + '/*'))
            
            file = 'images.zip'
            fileobj = open(zip_file, 'rb')
            
            update_status_work_request(mail, 'smart_crop')
            
            tr = threading.Thread(target=smart_crop_request, args=(mail, server, session, file, fileobj))
            tr.start()
                
            return jsonify(message='Smart crop started', category="success", status=200)
        
        return jsonify(message='Training already running', category="error", status=500)

    return jsonify(message='Did not receive a POST request', category="error", status=500)


def is_training_running(mail):
    return len(work_requests.loc[(work_requests['mail'] == mail) & (work_requests['status'] != 'completed')]) > 0

def extract_fields_from_url(url):
    match = re.search(r'\/n\/(?P<namespace>\w+)\/b\/(?P<bucket>\w+)\/o\/(?P<mail>\w+@[\w.]+)\/(?P<filename>[\w.]+)', url)
    return match.groupdict()

def update_status_work_request(mail, status):
    work_requests.loc[work_requests['mail'] == mail, 'status'] = status
    work_requests.to_csv(WORK_REQUEST_FILE, index=False)

def smart_crop_request(mail, server, session, file, fileobj):
    time.sleep(10) # Wait to process gets to wait activity
    r = requests.post('http://' + server +':6000/', data={'session': session}, files={"images": (file, fileobj)})
            
    if r.status_code == 200:
        zip_ready_file = 'sessions/' + session + '/images_ready.zip'
        with open(zip_ready_file, 'wb') as f:
            f.write(r.content)
        update_status_work_request(mail, 'smart_crop_completed')
    else:
        update_status_work_request(mail, 'smart_crop_failed')
        

@flask.route('/train', methods=['POST'])
def train():
    global work_requests
    if request.method == 'POST':
        content = request.get_json()
        mail = content['mail']
        session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
        zip_ready_file = 'sessions/' + session + '/images_ready.zip'
        
        update_status_work_request(mail, 'training_started')
        
        tr = threading.Thread(target=start_training, args=(mail, zip_ready_file, session))
        tr.start()
        
        return jsonify({'status': 'success', 'message': 'Training started'})
    
    
    return jsonify({'status': 'error', 'message': 'Invalid request'})
        

def start_training(mail, zip_file, server ,session):
    training_subject = 'Character'
    subject_type = 'person'
    class_dir = 'person_ddim'
    training_steps = 100
    seed = random.randint(7, 1000000)
    fileobj = open(zip_file, 'rb')
    payload = {'training_subject': training_subject, 'subject_type': subject_type, 'instance_name': session, 'class_dir': class_dir, 'training_steps': training_steps, 'seed': seed}
    
    update_status_work_request(mail, 'training_started')
    
    r = requests.post('http://' + server + ':3000/', data=payload, files={"images": (zip_file, fileobj)})
    
    if r.status_code == 200:
        task_training = sched.scheduler(time.time, time.sleep)
        task_training.enter(300, 1, check_if_training, (task_training,))
        task_training.run()
    else:
        update_status_work_request(mail, 'train_failed')


def check_if_training(runnable_task, mail):
    global work_requests
    server = work_requests.loc[work_requests['mail'] == mail, 'server'].values[0]
    if is_dreambooth_running(server):
        runnable_task.enter(300, 1, check_if_training, (runnable_task, mail))
    else:
        runnable_task.enter(900, 1, sd_ready, (runnable_task, mail))


def sd_ready(runnable_task, mail):
    global work_requests
    tag = work_requests.loc[work_requests['mail'] == mail, 'tag'].values[0]
    session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
    SESSION_DIR = 'sessions/' + session
    
    server = work_requests.loc[work_requests['mail'] == mail, 'server'].values[0]
    
    file_prompt = prompts.loc[prompts['tag'] == tag, 'file'].values[0]
    
    new_file_prompt = SESSION_DIR + '/' + '/prompts.json'
    subprocess.getoutput("cp " + file_prompt + " " + new_file_prompt)
    subprocess.getoutput('sed -i "s/<subject>/' + session + '/g" ' + new_file_prompt)
    fileobj = open(new_file_prompt, 'rb')

    update_status_work_request(mail, 'image_generation_started')
    
    r = requests.post('http://' + server + ':3000/txt2img', files={"prompts": ('prompts.json', fileobj)})
    
    if r.status_code == 200:
        zip_ready_generated = 'sessions/' + session + '/images_generated.zip'
        with open(zip_ready_generated, 'wb') as f:
            f.write(r.content)
        update_status_work_request(mail, 'image_generation_completed')
    else:
        update_status_work_request(mail, 'image_generation_failed')

def is_dreambooth_running(server):
    r = requests.get('http://' + server + ':3000/status')
    return r.status_code == 200 and r.json()['status']

# run the flask app
if __name__ == '__main__':
    flask.run(debug=True, port=7000)
    


