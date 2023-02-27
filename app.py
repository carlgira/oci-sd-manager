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
import data
logging.basicConfig(filename='output.log', encoding='utf-8', level=logging.INFO)

flask = Flask(__name__)
cors = CORS(flask)
cors_origins = ['*']

object_storage_client = ObjectStorageClient(config=oci.config.from_file('~/.oci/config'))

@flask.route('/work_requests', methods=['PUT', 'GET', 'POST'])
def work_requests_api():    
    if request.method == 'PUT':
        content = request.get_json()
        mail = content['mail']
        status = content['status']
        
        data.update_status_work_request(mail, status)
    
        return jsonify({'status': 'success', 'message': 'Work request updated'})

    if request.method == 'GET':
        mail = request.args.get('mail')
        if mail is None or len(mail) == 0:
            return jsonify(data.get_work_requests())
        
        decoded_mail = mail.replace('%40', '@')
        return jsonify(data.get_work_request(decoded_mail))
        

@flask.route('/servers', methods=['GET', 'POST', 'DELETE'])
def servers_api():
    if request.method == 'GET':
        data.check_servers()
        return jsonify(data.get_servers())

    if request.method == 'POST':
        content = request.get_json()
        ip = content['ip']
        status = content['status']
        data.add_new_server(ip, status)
        return jsonify({'status': 'success', 'message': 'Server added'})
    
    if request.method == 'DELETE':
        content = request.get_json()
        ip = content['ip']
        data.delete_server(ip)
        return jsonify({'status': 'success', 'message': 'Server deleted'})
    
    return jsonify({'status': 'error', 'message': 'Invalid request'})


@flask.route('/event', methods=['POST'])
def get_events():
    if request.method == 'POST':
        content = request.get_json()
        event = content['event']

        return jsonify(data.get_event(event))

@flask.route('/submit_images', methods=['POST'])
def submit_images():
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
            
            if len(data.get_work_request(mail)) == 0:
                data.add_new_work_request(mail, server, tag, session, 'created', event)
            
            session = data.get_work_request(mail, 'session')
            server = data.get_work_request(mail, 'server')
            
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
            
            data.update_status_work_request(mail, 'smart_crop_started')
            data.update_status_server(server, 'busy')
            
            tr = threading.Thread(target=smart_crop_request, args=(mail, server, session, file, fileobj))
            tr.start()
                
            return jsonify(message='Smart crop started', category="success", status=200)
        
        data.update_status_work_request(mail, 'smart_crop_failed')
        return jsonify(message='Training already running', category="error", status=500)

    return jsonify(message='Did not receive a POST request', category="error", status=500)


mail_regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,7}\b'
def check_mail(mail):
    return re.fullmatch(mail_regex, mail)

@flask.route('/clean_mail', methods=['POST'])
def clean_mail():
    if request.method == 'POST':
        content = request.get_json()
        mail = content['mail']
        if not check_mail(mail):
            return jsonify(message='Invalid mail', category="error", status=500)
        else:
            files = object_storage_client.list_objects(os.environ['NAMESPACE_NAME'], 
                                                       os.environ['BUCKET_NAME'], prefix=mail).data.objects
            for file in files:
                print(file)
                object_storage_client.delete_object(os.environ['NAMESPACE_NAME'], 
                                                os.environ['BUCKET_NAME'], file['name'])
            
            data.delete_work_request(mail)
            
        return jsonify(message='Mail cleaned', category="success", status=200)
    
    return jsonify(message='Did not receive a POST request', category="error", status=500)

def is_training_running(mail):
    work_request = data.get_work_request(mail)
    if len(work_request) == 0:
        return False
    return work_request['status'] != 'completed' or work_request['status'] != 'smart_crop_failed'

def extract_fields_from_url(url):
    url = url.split('/')
    namespace = url[-6]
    bucket = url[-4]
    folder = url[-2]
    filename = url[-1]
    return {"namespace": namespace, "bucket": bucket, "mail": folder, "filename": filename}

@flask.route('/smart_crop', methods=['POST'])
def smart_crop():
    if request.method == 'POST':
        
        content = request.get_json()
        mail = content['mail']
        server = data.get_work_request(mail, 'server')
        session = data.get_work_request(mail, 'session')
        file = 'images.zip'
        SESSION_DIR = 'sessions/' + session
        zip_file = SESSION_DIR + '/' + file
        fileobj = open(zip_file, 'rb')
        
        tr = threading.Thread(target=smart_crop_request, args=(mail, server, session, file, fileobj))
        tr.start()
                
        return jsonify(message='Smart crop started', category="success", status=200)

    return jsonify(message='Did not receive a POST request', category="error", status=500)
    
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
            data.update_status_work_request(mail, 'smart_crop_completed')
        else:
            logging.info('Smart crop failed ' + r.text)
            data.update_status_work_request(mail, 'smart_crop_failed')
        
    except:
        data.update_status_work_request(mail, 'smart_crop_failed')
        traceback.print_exc()
        

@flask.route('/train', methods=['POST'])
def train():
    if request.method == 'POST':
        content = request.get_json()
        mail = content['mail']
        session = data.get_work_request(mail, 'session')
        server = data.get_work_request(mail, 'server')
        zip_ready_file = 'sessions/' + session + '/images_ready.zip'
        
        data.update_status_work_request(mail, 'training_started')
        data.update_status_server(server, 'busy')
        
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
    
    data.update_status_work_request(mail, 'training_started')
    
    r = requests.post('http://' + server + ':3000/', data=payload, files={"images": (zip_file, fileobj)})
    logging.info('Training started ')
    
    if r.status_code == 200:
        task_training = sched.scheduler(time.time, time.sleep)
        task_training.enter(60, 1, check_if_training, (task_training, mail,))
        task_training.run()
    else:
        data.update_status_work_request(mail, 'train_failed')

def check_if_training(runnable_task, mail):
    server = data.get_work_request(mail, 'server')
    if is_dreambooth_running(server):
        logging.info('Still training')
        runnable_task.enter(60, 1, check_if_training, (runnable_task, mail))
    else:
        logging.info('SD ready')
        data.update_status_work_request(mail, 'training_completed')

@flask.route('/sd_ready', methods=['POST'])
def sd_ready_api():
    content = request.get_json()
    mail = content['mail']
    sd_ready(mail)
    return jsonify({'status': 'success', 'message': 'SD executed'})

def sd_ready(mail):
    tag = data.get_work_request(mail, 'tag')
    session = data.get_work_request(mail, 'session')
    SESSION_DIR = 'sessions/' + session
    
    server = data.get_work_request(mail, 'server')
    file_prompt = data.get_prompt(tag, 'file_path')
    
    new_file_prompt = SESSION_DIR + '/' + '/prompts.json'
    subprocess.getoutput("cp " + file_prompt + " " + new_file_prompt)
    subprocess.getoutput('sed -i "s/<subject>/' + session + '/g" ' + new_file_prompt)
    fileobj = open(new_file_prompt, 'rb')

    data.update_status_work_request(mail, 'image_generation_started')
    
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
        
        data.update_status_work_request(mail, 'image_generation_completed')
    else:
        data.update_status_work_request(mail, 'image_generation_failed')
    

def is_dreambooth_running(server):
    r = requests.get('http://' + server + ':3000/status')
    return r.status_code == 200 and r.json()['status']

@flask.route('/collage', methods=['POST'])
def collage():
    content = request.get_json()
    mail = content['mail']
    session = data.get_work_request(mail, 'session')
    event = data.get_work_request(mail, 'event')
    generated_images_dir = 'sessions/' + session + '/' + mail + '_generated_images'
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
        
    img_logo_path = data.get_event(event, 'image_path')
    logo = Image.open(img_logo_path)
    final = Image.alpha_composite(new_im, logo)
    
    final.save(working_images_dir + '/collage.png')
    
    return working_images_dir + '/collage.png'

@flask.route('/chosen_images', methods=['POST'])
def chosen_images():
    content = request.get_json()
    mail = content['mail']
    files = content['files']
    session = data.get_work_request(mail, 'session')
    
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
    
    event = data.get_work_request(mail, 'event')
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
    event = data.get_work_request(mail, 'event')
    session = data.get_work_request(mail, 'session')
    server = data.get_work_request(mail, 'server')
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
    
    data.update_status_server(server, 'free')
    
    return jsonify({'status': 'success', 'message': 'Images ready for user', 'url': "https://objectstorage.eu-frankfurt-1.oraclecloud.com" + response.data.access_uri})


# run the flask app
if __name__ == '__main__':
    flask.run(host='0.0.0.0', port=7000)
    


