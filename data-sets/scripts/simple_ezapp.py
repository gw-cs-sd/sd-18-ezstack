"""
Python Version: Python 3

Simple Python script for reading, writing documents from ezapp.
"""
import requests
import argparse
import uuid
import json
import threading
import time
import sys

debug = False


def chunk(l, n):
    '''Split the list, l, into n chunks'''
    L = len(l)
    assert 0 < n <= L
    s = L//n
    return [l[p:p + s] for p in range(0, L, s)]

def debug_print(*argv):
    if debug == True:
        print(argv)

def get_json(r):
    try:
        return r.json()
    except:
        return None

def single_doc_write(list_docs, url, table):
    for doc in list_docs:
        if 'id' in doc:
            r = requests.post('{}/sor/1/{}/{}'.format(url, table, doc['id']), data=json.dumps(doc), headers={'content-type': 'application/json'})
        else:
            r = requests.post('{}/sor/1/{}/{}'.format(url, table, uuid.uuid4()), data=json.dumps(doc), headers={'content-type': 'application/json'})
        debug_print(r.text)

def single_doc_update(list_docs, url, table):
    for doc in list_docs:
        if 'id' in doc:
            r = requests.put('{}/sor/1/{}/{}'.format(url, table, doc['id']), data=json.dumps(doc), headers={'content-type': 'application/json'})
        else:
            r = requests.put('{}/sor/1/{}/{}'.format(url, table, uuid.uuid4()), data=json.dumps(doc), headers={'content-type': 'application/json'})
        debug_print(r.text)

def get_document(list_ids, url, table):
    ret = []
    for doc in list_ids:
        if isinstance(doc, dict):
            r = requests.get('{}/sor/1/{}/{}'.format(url, table, doc['id']))
        else:
            r = requests.get('{}/sor/1/{}/{}'.format(url, table, doc))
        ret.append(get_json(r))
        debug_print(get_json(r))
    return ret

def setup_argparse(parser=None):
    if parser == None:
        parser = argparse.ArgumentParser(description='Simple Python script for reading, writing documents to ezapp')
    parser.add_argument('-w', '--write', dest='write', action='store_true', default=False, help='writes documents to ezapp')
    parser.add_argument('-u', '--update', dest='update', action='store_true', default=False, help='update documents to ezapp')
    parser.add_argument('-g', '--get', dest='get', action='store_true', default=False, help='gets the following documents from ezapp')
    parser.add_argument('-t', '--table', dest='table', help='sets table name otherwise uses default')
    parser.add_argument('-p', '--path', dest='path', help='specifies the location of the json file (required for '
                                                          'majority of options)')
    parser.add_argument('--url', dest='url', default='http://127.0.0.1:8080', help='sets url otherwise defaults to localhost')
    parser.add_argument('-threads', '--threads', dest='num_threads', default=5, type=int, help='set the number of threads to run')
    parser.add_argument('--debug', dest='debug', action='store_true', default=False, help='enables debug mode (adds more print statments)')
    return parser

if __name__=='__main__':
    parser = setup_argparse()
    args = parser.parse_args()

    documents = []
    table = 'test_table'
    debug = args.debug
    url = args.url

    if args.path is not None:
        f = open(args.path, 'r')
        documents = chunk(json.loads(''.join(f.readlines())), args.num_threads)
    if args.table is not None:
        table = args.table
    if args.write is True:
        try:
            for partial_docs in documents:
                threading.Thread(target=single_doc_write, args=(partial_docs, url, table)).start()
        except Exception as e:
            print(e)
    if args.update is True:
        try:
            for partial_docs in documents:
                threading.Thread(target=single_doc_update, args=(partial_docs, url, table)).start()
        except Exception as e:
            print(e)
    if args.get is True:
        try:
            for partial_docs in documents:
                threading.Thread(target=get_document, args=(partial_docs, url, table)).start()
        except Exception as e:
            print(e)

    while 1:
        if (threading.active_count() <= 1):
            sys.exit()
        time.sleep(1)

