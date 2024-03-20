import json
import urllib3
import boto3
from datetime import datetime as dt


def lambda_handler(event, context):
	s3 = boto3.client('s3')
	bucket = 'currency-scd2-bucket'
	key = 'currExch_{}.txt'.format(dt.now().strftime("%Y-%m-%d %H:%M:%S"))
	file_name = '/tmp/testfile1.txt'
	from_code = event['curr_from']
	
	objects = s3.list_objects_v2(Bucket=bucket)
	if 'Contents' in objects:
		for obj in objects['Contents']:
			s3.delete_object(Bucket=bucket, Key=obj['Key'])

	json_data = getData(from_code)
	l=len(json_data)
    
	with open(file_name,'w') as f:
		for i,t in enumerate(json_data.items()):
			f.write('{},{},{},{}'.format(from_code,t[0],round(t[1],2),dt.now().strftime("%Y-%m-%d")))
			if i<l-1:
				f.write('\n')

	s3.upload_file(file_name, bucket, key)
	# data = s3.get_object(Bucket=bucket, Key=key)
	return json_data

def getData(from_code):
	url = 'https://open.er-api.com/v6/latest/'+from_code
	http = urllib3.PoolManager()
	try:
		response = http.request('GET', url)
		if response.status == 200:
			data = response.data.decode('utf-8')
			json_data = json.loads(data)['rates']
		else:
			print(f"Failed to fetch data. Status code: {response.status}")
	except Exception as e:
		print(f"Error: {e}")
	
	return json_data
