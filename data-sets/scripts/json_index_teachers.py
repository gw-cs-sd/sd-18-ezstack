import json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
es = Elasticsearch()

path = '../student_teacher/MOCK_DATA_TEACHERS.json'

f=open(path, 'r')
content = f.readlines()
data = ''.join(content)
data = json.loads(data)
f.close()

# i=0
# for item in data:
# 	doc = item
# 	res = es.index(index='students', doc_type='student', id=doc['id'], body=doc)
# 	i+=1

actions = [
	{
		'_index': 'teachers',
		'_type': 'teacher',
		'_id': doc['id'],
		'_source': doc
	}
	for doc in data
]

helpers.bulk(es, actions)