import cx_Oracle
import os
import requests
import json
import logging

username = os.environ["ATP_USERNAME"]
password = os.environ["ATP_PASSWORD"]
db_url = os.environ["DB_DNS"]

WORK_REQUESTS_TABLE = 'WORK_REQUEST'
SERVERS_TABLE = 'SERVERS'
PROMPTS_TABLE = 'PROMPTS'
EVENTS_TABLE = 'EVENTS'

# WORK REQUESTS

def get_work_request(mail, field=None):
    if field is None:
        return get_data_id(WORK_REQUESTS_TABLE, mail)
    return get_data_id(WORK_REQUESTS_TABLE, mail)[field]

def get_work_requests():
    return get_data_all(WORK_REQUESTS_TABLE)

def add_new_work_request(mail, server, tag, session, status, event):
    new_work_request = {'id': mail, 'mail': mail, 'server': server, 'tag': tag, 'session': session, 'status': status, 'event': event}
    insert_data(WORK_REQUESTS_TABLE, new_work_request)

def update_status_work_request(mail, status):
    update_data(WORK_REQUESTS_TABLE, mail, 'status', status)

# SERVERS

def get_servers():
    return get_data_all(SERVERS_TABLE)

def add_new_server(server_ip, server_status):
    new_server = {'id': server_ip, 'ip': server_ip, 'status': server_status}
    insert_data(SERVERS_TABLE, new_server)

def delete_server(server_ip):
    delete_data(SERVERS_TABLE, server_ip)

def update_status_server(server, status):
    update_data(SERVERS_TABLE, server, 'status', status)

# PROMPTS

def get_prompts():
    return get_data_all(PROMPTS_TABLE)

def get_prompt(prompt_id, field=None):
    if field is None:
        return get_data_id(PROMPTS_TABLE, prompt_id)
    return get_data_id(PROMPTS_TABLE, prompt_id)[field]

# EVENTS

def get_events():
    return get_data_all(EVENTS_TABLE)

def get_event(event_id, field=None):
    if field is None:
        return get_data_id(EVENTS_TABLE, event_id)
    return get_data_id(EVENTS_TABLE, event_id)[field]

def check_servers():
    for server in get_data_all(SERVERS_TABLE):
        server = server['ip']
        try:
            response = requests.get('http://' + server + ':3000/status', timeout=3)
            if response.status_code != 200:
                delete_server(server)
        except:
            delete_server(server)

# database

def get_data_id(table, id):
    con = None
    try:
        con = cx_Oracle.connect(username, password, db_url)
        with con.cursor() as cursor:
            r = cursor.execute("SELECT c.doc FROM {USER}.{TABLE} c where c.doc.id = '{ID}'".format(USER=username, TABLE=table, ID=id))
            for response in r:
                value = json.loads(response[0].read())
                return value
    except Exception:
        logging.exception("ATP Error")
    finally:
        if con is not None:
            con.close()

def get_data_all(table):
    con = None
    try:
        con = cx_Oracle.connect(username, password, db_url)
        all_response = []
        with con.cursor() as cursor:
            cursor.execute("SELECT c.doc FROM {USER}.{TABLE} c".format(USER=username, TABLE=table))
            res = cursor.fetchall()
            for row in res:
                all_response.append(json.loads(row[0].read()))
        return all_response
    except Exception:
        logging.exception("ATP Error")
    finally:
        if con is not None:
            con.close()

def update_data(table, id, field, value):
    con = None
    try:
        con = cx_Oracle.connect(username, password, db_url)
        doc_json = get_data_id(table, id)
        doc_json[field] = value
        with con.cursor() as cursor:
            cursor.execute("UPDATE {USER}.{TABLE} c SET c.doc = '{DATA}' WHERE c.doc.id = '{ID}'".format(USER=username, TABLE=table, DATA=json.dumps(doc_json), ID=id))
            con.commit()
    except Exception:
        logging.exception("ATP Error")
    finally:
        if con is not None:
            con.close()
            
def delete_data(table, id):
    con = None
    try:
        con = cx_Oracle.connect(username, password, db_url)
        with con.cursor() as cursor:
            cursor.execute("DELETE FROM {USER}.{TABLE} c WHERE c.doc.id = '{ID}'".format(USER=username, TABLE=table, ID=id))
            con.commit()
    except Exception:
        logging.exception("ATP Error")
    finally:
        if con is not None:
            con.close()

def insert_data(table, data):
    con = None
    try:
        con = cx_Oracle.connect(username, password, db_url)
        with con.cursor() as cursor:
            cursor.execute("INSERT INTO {USER}.{TABLE} (doc) VALUES ('{DATA}')".format(USER=username, TABLE=table, DATA=str(data).replace("'", '\"')))
            con.commit()
    except Exception:
        logging.exception("ATP Error")
    finally:
        if con is not None:
            con.close()