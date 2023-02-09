import cx_Oracle
import os
import pandas as pd
import requests

WORK_DIR = os.environ['install_dir']
SEVERS_FILE= 'data/servers.csv'
WORK_REQUEST_FILE = 'data/work_requests.csv'
PROMPTS_FILE = 'data/prompts.csv'
EVENTS_FILE = 'data/events.csv'

servers = pd.read_csv(SEVERS_FILE)
work_requests = pd.read_csv(WORK_REQUEST_FILE)
prompts = pd.read_csv(PROMPTS_FILE)
events = pd.read_csv(EVENTS_FILE)

# WORK REQUESTS

def get_work_request(mail, field):
    if field is None:
        return work_requests[work_requests['mail'] == mail].to_dict(orient='records')    
    return work_requests[work_requests['mail'] == mail][field].values[0]

def get_work_requests():
    return work_requests.to_dict(orient='records')

def add_new_work_request(mail, server, tag, session, status, event):
    new_work_request = {'mail': mail, 'server': server, 'tag': tag, 'session': session, 'status': status, 'event': event}
    work_requests.append(new_work_request, ignore_index=True)
    work_requests.to_csv(WORK_REQUEST_FILE, index=False)

def update_status_work_request(mail, status):
    work_requests.loc[work_requests['mail'] == mail, 'status'] = status
    work_requests.to_csv(WORK_REQUEST_FILE, index=False)

# SERVERS

def get_servers():
    return servers.to_dict(orient='records')

def get_server(server_name):
    return servers[servers['ip'] == server_name].to_dict(orient='records')

def add_new_server(server_ip, server_status):
    new_server = {'ip': server_ip, 'status': server_status}
    servers.append(new_server, ignore_index=True)
    servers.to_csv(SEVERS_FILE, index=False)

def delete_server(server_ip):
    servers = servers[servers.ip != server_ip]
    servers.to_csv(SEVERS_FILE, index=False)

def update_status_server(server, status):
    servers.loc[servers['ip'] == server, 'status'] = status
    servers.to_csv(SEVERS_FILE, index=False)

# PROMPTS

def get_prompts():
    return prompts.to_dict(orient='records')

def get_prompt(prompt_id, field):
    if field is None:
        return prompts[prompts['tag'] == prompt_id].to_dict(orient='records')
    return prompts[prompts['tag'] == prompt_id][field].values[0]

# EVENTS

def get_events():
    return events.to_dict(orient='records')

def get_event(event_id, field):
    if field is None:
        return events[events['event'] == event_id].to_dict(orient='records')
    return events[events['event'] == event_id][field].values[0]

def check_servers():
    for server in servers['ip'].values:
        try:
            response = requests.get('http://' + server + ':3000/status', timeout=3)
            if response.status_code != 200:
                servers = servers[servers['ip'] != server]
        except:
            servers = servers[servers['ip'] != server]
    
    servers.to_csv(SEVERS_FILE, index=False)