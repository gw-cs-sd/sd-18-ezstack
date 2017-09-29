import json

output=[]

# concat files into list
for i in range (0, 50):
	f=open('MOCK_DATA%s.json' % i, 'r')
	content = f.readlines()
	data = ''.join(content)
	data = json.loads(data)
	output.extend(data)
	f.close()

# change json id then write back
i=0
for item in output:
	item['id']=i
	i+=1
file=open('MOCK_DATA.json', 'w')
file.write(json.dumps(output))
file.close()


# Test section
file=open('MOCK_DATA.json', 'r')
output=json.loads(''.join(file.readlines()))

# Printing the json objects
"""
for item in output:
	print json.dumps(item)
"""