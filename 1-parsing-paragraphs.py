from google.cloud import storage, bigquery
import re

#download debate from cloud-storage
client = storage.Client()
bucket = client.get_bucket('serge-playground.appspot.com')
blob = bucket.blob('debate.txt')
debate = blob.download_as_string()

#regex debate to usable format
without_brackets = re.sub(r'\((.*?)\)', ' ', debate)
split_for_speaker = re.split(r'(TRUMP: |CLINTON: |WALLACE: )', without_brackets) 

#connect to BQ
client = bigquery.Client(project='serge-playground')
dataset = client.dataset('election_nlp_vegas')
dataset.create()             

#define Schema
id = bigquery.SchemaField('id', 'integer')
name = bigquery.SchemaField('name', 'string')
sentence = bigquery.SchemaField('sentence', 'string')

#create table
table = dataset.table('paragraphs', schema=[id, name, sentence])
table.create() 

#insert debate into BQ
rows_to_insert = [];
for x in range(1, len(split_for_speaker), 2):
	print split_for_speaker[x]
	rows_to_insert.append([x, split_for_speaker[x], split_for_speaker[x+1]])
	
table.insert_data(rows_to_insert)

