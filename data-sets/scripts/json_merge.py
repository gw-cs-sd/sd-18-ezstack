import json, os

output = []
filepath = './'

# concat files into list
json_files = [pos_json for pos_json in os.listdir(filepath) if pos_json.endswith('.json')]
for file in json_files:
	f=open(file, 'r')
	content = f.readlines()
	data = ''.join(content)
	data = json.loads(data)
	output.extend(data)
	f.close()

# change json id then write back
i = 0
for item in output:
	item['id'] = i
	i += 1
file=open('MOCK_DATA.json', 'w')
file.write(json.dumps(output, indent = 4, separators = (',',':')))
file.close()

# Test section
file=open('MOCK_DATA.json', 'r')
output=json.loads(''.join(file.readlines()))

# Printing the json objects
"""
for item in output:
	print json.dumps(item)
"""