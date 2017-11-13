import json
import requests
import argparse
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import thread

from multiprocessing.dummy import Pool as ThreadPool
es = Elasticsearch()

path = '../student_teacher/MOCK_DATA.json'

headers = {
    'Content-Type': 'application/json',
}

parser = argparse.ArgumentParser(description='Sends individual documents into elasticsearch.')
parser.add_argument('-t', '--table', dest='table', required=True, help='Specify the table that the document belongs in.')

args = parser.parse_args()

f=open(path, 'r')
content = f.readlines()
data = ''.join(content)
data = json.loads(data)
f.close()

def write(doc):
	requests.post('http://localhost:8080/sor/1/{}/{}'.format(args.table, doc['id']), headers=headers, data=json.dumps(doc))


pool = ThreadPool(100)
pool.map(write, data)


#for doc in data:
#	requests.post('http://localhost:8080/sor/1/{}/{}'.format(args.table, doc['id']), headers=headers, data=json.dumps(doc))


# i=0
# for doc in data:
# 	if doc['id'] % 5 == 0:
# 		thread.start_new_thread(write, (doc['id']))
# 	elif doc['id']
 	#res = es.index(index='students', doc_type='student', id=doc['id'], body=doc)
 	# r = requests.post('http://localhost:8080/sor/1/{}/{}'.format(args.table, doc['id']), headers=headers, data=json.dumps(doc))
#actions = [
#	{
#		'_index': 'students',
#		'_type': 'student',
#		'_id': doc['id'],
#		'_source': doc
#	}
#	for doc in data
#]
#
#helpers.bulk(es, actions)