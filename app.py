from flask import Flask, render_template, request, jsonify, send_file
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
from oci.object_storage.models import CreatePreauthenticatedRequestDetails
import sched
import logging
from PIL import Image
import datetime
import traceback
logging.basicConfig(filename='output.log', encoding='utf-8', level=logging.INFO)

WORK_DIR = os.environ['install_dir']
SEVERS_FILE= 'data/servers.csv'
WORK_REQUEST_FILE = 'data/work_requests.csv'
PROMPTS_FILE = 'data/prompts.csv'
EVENTS_FILE = 'data/events.csv'

servers = pd.read_csv(SEVERS_FILE)
work_requests = pd.read_csv(WORK_REQUEST_FILE)
prompts = pd.read_csv(PROMPTS_FILE)
events = pd.read_csv(EVENTS_FILE)

flask = Flask(__name__)
cors = CORS(flask)
OIC_URL = os.environ['OIC_URL']
cors_origins = ['*']

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
        if mail is None or len(mail) == 0:
            return jsonify(work_requests.to_dict(orient='records'))
        
        decoded_mail = mail.replace('%40', '@')
        return jsonify(work_requests.loc[work_requests['mail'] == decoded_mail].to_dict(orient='records'))
        

@flask.route('/servers', methods=['GET', 'POST', 'DELETE'])
def servers_api():
    global servers
    if request.method == 'GET':
        check_servers()
        return jsonify(servers.to_dict(orient='records'))

    if request.method == 'POST':
        content = request.get_json()
        ip = content['ip']
        status = content['status']
        servers = servers.append({'ip': ip, 'status': status}, ignore_index=True)
        servers.to_csv(SEVERS_FILE, index=False)
        return jsonify({'status': 'success', 'message': 'Server added'})
    
    if request.method == 'DELETE':
        content = request.get_json()
        ip = content['ip']
        servers = servers[servers.ip != ip]
        servers.to_csv(SEVERS_FILE, index=False)
        return jsonify({'status': 'success', 'message': 'Server deleted'})
    
    return jsonify({'status': 'error', 'message': 'Invalid request'})


@flask.route('/submit_images', methods=['POST'])
def submit_images():
    global work_requests
    if request.method == 'POST':
        
        content = request.get_json()
        mail = content['mail']
        server = content['server']
        tag = content['tag']
        event = content['event']
        
        images = content['images']
        session = ''.join(random.choice('abcdefghijklmnopqrtsvwyz') for i in range(10))
        
        SESSION_DIR = 'sessions/' + session
        os.mkdir(SESSION_DIR)
        
        if not is_training_running(mail):
            if images is None or len(images) == 0:
                return jsonify(message='No file uploaded', category="error", status=500)
            
            if not work_requests['mail'].str.contains(mail).any():    
                work_requests = work_requests.append({'mail': mail, 'server': server, 'tag': tag, 'session' : session, 'status': 'created', 'event' : event}, ignore_index=True)
                work_requests.to_csv(WORK_REQUEST_FILE, index=False)
            
            session = work_requests.loc[work_requests['mail'] == mail]['session'].values[0]
            server = work_requests.loc[work_requests['mail'] == mail]['server'].values[0]
            
            url_parts = extract_fields_from_url(images[0])
            namespace = url_parts['namespace']
            bucket = url_parts['bucket']
            
            list_object_versions = object_storage_client.list_objects(namespace, bucket, prefix=mail).data.objects
            
            for i, img_url in enumerate(list_object_versions):
                filename = img_url.name.split('/')[-1]
                extension = filename.split('.')[-1]
                
                img_content = object_storage_client.get_object(namespace, bucket, mail + '/' + filename).data.content
                
                original_images = SESSION_DIR + '/' + mail
                if not os.path.exists(original_images):
                    os.mkdir(original_images)
                with open(original_images + '/' + str(i) + '.' + extension, 'wb') as f:
                    f.write(img_content)
            
            file = 'images.zip'    
            zip_file = SESSION_DIR + '/' + file
            subprocess.getoutput("zip -j {ZIP_FILE} {ZIP_FILES}".format(ZIP_FILE=zip_file, ZIP_FILES=original_images + '/*'))
            
            fileobj = open(zip_file, 'rb')
            
            update_status_work_request(mail, 'smart_crop_started')
            update_status_server(server, 'busy')
            
            tr = threading.Thread(target=smart_crop_request, args=(mail, server, session, file, fileobj))
            tr.start()
                
            return jsonify(message='Smart crop started', category="success", status=200)
        
        update_status_work_request(mail, 'smart_crop_failed')
        return jsonify(message='Training already running', category="error", status=500)

    return jsonify(message='Did not receive a POST request', category="error", status=500)


def is_training_running(mail):
    return len(work_requests.loc[(work_requests['mail'] == mail) & (work_requests['status'] != 'completed') & (work_requests['status'] != 'smart_crop_failed')]) > 0

def extract_fields_from_url(url):
    url = url.split('/')
    namespace = url[-6]
    bucket = url[-4]
    folder = url[-2]
    filename = url[-1]
    return {"namespace": namespace, "bucket": bucket, "mail": folder, "filename": filename}

def update_status_work_request(mail, status):
    work_requests.loc[work_requests['mail'] == mail, 'status'] = status
    work_requests.to_csv(WORK_REQUEST_FILE, index=False)

def update_status_server(server, status):
    servers.loc[servers['ip'] == server, 'status'] = status
    servers.to_csv(SEVERS_FILE, index=False)

def smart_crop_request(mail, server, session, file, fileobj):    
    try:
        r = requests.post('http://' + server +':4000/submit', data={'session': session}, files={"images": (file, fileobj)})
        if r.status_code == 200:
            zip_ready_file = 'sessions/' + session + '/images_ready.zip'
            with open(zip_ready_file, 'wb') as f:
                f.write(r.content)
                
            crop_images_dir = 'sessions/' + session + '/' + mail + '_crop_images'
            
            if not os.path.exists(crop_images_dir):
                os.mkdir(crop_images_dir)
                
            subprocess.run(["unzip", "-o" , zip_ready_file, '-d' , crop_images_dir], check=True)
            
            for file in os.listdir(crop_images_dir):
                object_storage_client.put_object(
                    namespace_name=os.environ['NAMESPACE_NAME'],
                    bucket_name=os.environ['BUCKET_NAME'],
                    object_name=mail +  '_crop_images' + '/' + file,
                    put_object_body=open(crop_images_dir + '/' + file, 'rb')
                )
            
            logging.info('Smart crop completed ' + r.text)
            update_status_work_request(mail, 'smart_crop_completed')
        else:
            logging.info('Smart crop failed ' + r.text)
            update_status_work_request(mail, 'smart_crop_failed')
        
    except:
        update_status_work_request(mail, 'smart_crop_failed')
        traceback.print_exc()
        

@flask.route('/train', methods=['POST'])
def train():
    global work_requests
    if request.method == 'POST':
        content = request.get_json()
        mail = content['mail']
        session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
        server = work_requests.loc[work_requests['mail'] == mail, 'server'].values[0]
        zip_ready_file = 'sessions/' + session + '/images_ready.zip'
        
        update_status_work_request(mail, 'training_started')
        update_status_server(server, 'busy')
        
        tr = threading.Thread(target=start_training, args=(mail, zip_ready_file, server , session))
        tr.start()
        
        return jsonify({'status': 'success', 'message': 'Training started'})
    
    
    return jsonify({'status': 'error', 'message': 'Invalid request'})
        

def start_training(mail, zip_file, server ,session):
    training_subject = 'Character'
    subject_type = 'person'
    class_dir = 'person_ddim'
    training_steps = 1600
    seed = random.randint(7, 1000000)
    fileobj = open(zip_file, 'rb')
    payload = {'training_subject': training_subject, 'subject_type': subject_type, 'instance_name': session, 'class_dir': class_dir, 'training_steps': training_steps, 'seed': seed}
    
    update_status_work_request(mail, 'training_started')
    
    r = requests.post('http://' + server + ':3000/', data=payload, files={"images": (zip_file, fileobj)})
    logging.info('Training started ')
    
    if r.status_code == 200:
        task_training = sched.scheduler(time.time, time.sleep)
        task_training.enter(60, 1, check_if_training, (task_training, mail,))
        task_training.run()
    else:
        update_status_work_request(mail, 'train_failed')


def check_if_training(runnable_task, mail):
    global work_requests
    server = work_requests.loc[work_requests['mail'] == mail, 'server'].values[0]
    if is_dreambooth_running(server):
        logging.info('Still training')
        runnable_task.enter(60, 1, check_if_training, (runnable_task, mail))
    else:
        logging.info('SD ready')
        update_status_work_request(mail, 'training_completed')
        runnable_task.enter(300, 1, sd_ready, (runnable_task, mail))

@flask.route('/sd_ready', methods=['POST'])
def sd_ready_api():
    content = request.get_json()
    mail = content['mail']
    sd_ready(None, mail)
    return jsonify({'status': 'success', 'message': 'SD executed'})

def sd_ready(runnable_task, mail):
    global work_requests
    tag = work_requests.loc[work_requests['mail'] == mail, 'tag'].values[0]
    session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
    SESSION_DIR = 'sessions/' + session
    
    server = work_requests.loc[work_requests['mail'] == mail, 'server'].values[0]
    
    file_prompt = prompts.loc[prompts['tag'] == tag, 'file_path'].values[0]
    
    new_file_prompt = SESSION_DIR + '/' + '/prompts.json'
    subprocess.getoutput("cp " + file_prompt + " " + new_file_prompt)
    subprocess.getoutput('sed -i "s/<subject>/' + session + '/g" ' + new_file_prompt)
    fileobj = open(new_file_prompt, 'rb')

    update_status_work_request(mail, 'image_generation_started')
    
    r = requests.post('http://' + server + ':3000/txt2img', files={"prompts": ('prompts.json', fileobj)})
    
    if r.status_code == 200:
        zip_ready_generated = 'sessions/' + session + '/images_generated.zip'
        generated_images_dir = 'sessions/' + session + '/' + mail + '_generated_images'
        
        with open(zip_ready_generated, 'wb') as f:
            f.write(r.content)
        
        if not os.path.exists(generated_images_dir):
            os.mkdir(generated_images_dir)
        
        subprocess.run(["unzip", '-o', zip_ready_generated, '-d' , generated_images_dir], check=True)
        
        for file in os.listdir(generated_images_dir):
            object_storage_client.put_object(
                namespace_name=os.environ['NAMESPACE_NAME'],
                bucket_name=os.environ['BUCKET_NAME'],
                object_name=mail +  '_generated_images' + '/' + file,
                put_object_body=open(generated_images_dir + '/' + file, 'rb')
            )
        
        update_status_work_request(mail, 'image_generation_completed')
        update_status_server(server, 'free')
    else:
        update_status_work_request(mail, 'image_generation_failed')
    

def is_dreambooth_running(server):
    r = requests.get('http://' + server + ':3000/status')
    return r.status_code == 200 and r.json()['status']

@flask.route('/collage', methods=['POST'])
def collage():
    content = request.get_json()
    mail = content['mail']
    session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
    generated_images_dir = 'sessions/' + session + '/' + mail + '_generated_images'
    event = work_requests.loc[work_requests['mail'] == mail, 'event'].values[0]
    collage_path = create_collage(generated_images_dir, event)
    return send_file(collage_path, mimetype='image/png')

def create_collage(working_images_dir, event):
    files = os.listdir(working_images_dir)
    random.shuffle(files)
    small_images = []
    big_images = []
    for file in files:
        if file.endswith('json'):
            continue
        width, height = Image.open(working_images_dir + '/' + file).size
        if width == 512 and height == 512:
            small_images.append(working_images_dir + '/' + file)
        else:
            big_images.append(working_images_dir + '/' + file)
    
    new_im = Image.new('RGBA', (512 * 3, 512 + 768))
    for i, (file_small, file_big) in enumerate(zip(small_images[:3], big_images[:3])):
        im_small = Image.open(file_small)
        im_big = Image.open(file_big)
        new_im.paste(im_small, ((i % 3) * 512, 0))
        new_im.paste(im_big, ((i % 3) * 512, 512))
        
    img_logo_path = events[events['event'] == event].image_path.values[0]
    logo = Image.open(img_logo_path)
    final = Image.alpha_composite(new_im, logo)
    
    final.save(working_images_dir + '/collage.png')
    
    return working_images_dir + '/collage.png'

@flask.route('/chosen_images', methods=['POST'])
def chosen_images():
    content = request.get_json()
    mail = content['mail']
    files = content['files']
    session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
    
    SESSION_DIR = 'sessions/' + session
    
    chosen_images_dir = SESSION_DIR + '/' + mail + '_chosen_images'
    generated_images_dir = SESSION_DIR + '/' + mail + '_generated_images'
    subprocess.run(["rm", '-rf', chosen_images_dir], check=True)
    subprocess.run(["mkdir", chosen_images_dir], check=True)
    
    for file in files:
        objectName = file['objectName'].split('/')[-1]
        use = file['use']
        if use:
            subprocess.run(["cp", generated_images_dir + '/' + objectName, chosen_images_dir], check=True)
    
    event = work_requests.loc[work_requests['mail'] == mail, 'event'].values[0]
    create_collage(chosen_images_dir, event)
    
    for file in os.listdir(chosen_images_dir):
            object_storage_client.put_object(
                namespace_name=os.environ['NAMESPACE_NAME'],
                bucket_name=os.environ['BUCKET_NAME'],
                object_name=mail +  '_chosen_images' + '/' + file,
                put_object_body=open(chosen_images_dir + '/' + file, 'rb')
            )
    
    return jsonify({'status': 'success', 'message': 'Images chosen successfully'})

@flask.route('/images_for_user', methods=['POST'])
def images_for_user():
    content = request.get_json()
    mail = content['mail']
    event = work_requests.loc[work_requests['mail'] == mail, 'event'].values[0]
    session = work_requests.loc[work_requests['mail'] == mail, 'session'].values[0]
    SESSION_DIR = 'sessions/' + session
    
    chosen_images_dir = SESSION_DIR + '/' + mail + '_chosen_images'
    create_collage(chosen_images_dir, event)
    
    filename = mail + '_final_images.zip'
    zip_file = SESSION_DIR + '/' + filename
    subprocess.getoutput("rm -rf {ZIP_FILE}".format(ZIP_FILE=zip_file))
    subprocess.getoutput("zip -j {ZIP_FILE} {ZIP_FILES}".format(ZIP_FILE=zip_file, ZIP_FILES=chosen_images_dir + '/*'))
    
    object_storage_client.put_object(
        namespace_name=os.environ['NAMESPACE_NAME'],
        bucket_name=os.environ['BUCKET_NAME'],
        object_name=filename,
        put_object_body=open(zip_file, 'rb')
    )
    
    pre_auth_request_details = CreatePreauthenticatedRequestDetails(
            name="download",
            bucket_listing_action="Deny",
            object_name=filename,
            access_type="ObjectRead",
            time_expires=datetime.datetime.now() + datetime.timedelta(days=3)
        )
    
    response = object_storage_client.create_preauthenticated_request(
        os.environ['NAMESPACE_NAME'], 
        os.environ['BUCKET_NAME'],
        pre_auth_request_details)
    
    return jsonify({'status': 'success', 'message': 'Images ready for user', 'url': "https://objectstorage.eu-frankfurt-1.oraclecloud.com" + response.data.access_uri})


def check_servers():
    global servers
    for server in servers['ip'].values:
        try:
            response = requests.get('http://' + server + ':3000/status')
            if response.status_code != 200:
                servers = servers[servers['ip'] != server]
        except:
            servers = servers[servers['ip'] != server]
    
    servers.to_csv(SEVERS_FILE, index=False)
    

# run the flask app
if __name__ == '__main__':
    flask.run(host='0.0.0.0', port=7000)
    


