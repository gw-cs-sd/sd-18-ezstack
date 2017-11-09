import json, requests
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch()

path = '../student_teacher/MOCK_DATA.json'

headers = {
    'Content-Type': 'application/json',
}

f=open(path, 'r')
content = f.readlines()
data = ''.join(content)
data = json.loads(data)
f.close()
table = ''

i=0
for doc in data:
 	info = '{"author":"Ben and Sam","title":"Chris is awesome","rating":290}'
 	#res = es.index(index='students', doc_type='student', id=doc['id'], body=doc)
 	requests.post('http://localhost:8080/sor/1/{}/{}'.format(table, doc['id']), headers=headers, data=info)
 	i+=1

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