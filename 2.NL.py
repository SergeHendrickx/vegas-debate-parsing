from google.cloud import storage, bigquery, language
import re, csv

#download debate form cloud-storage
storage_client = storage.Client()
bucket = storage_client.get_bucket('serge-playground.appspot.com')
blob = bucket.blob('debate.txt')
debate = blob.download_as_string()

#regex debate to usable format
without_brackets = re.sub(r'\((.*?)\)', ' ', debate)
#split_for_speaker = re.split("([A-Z][A-Z]+)", without_brackets) #doesnt wrk becouse of NBC, VAT
split_for_speaker = re.split(r'(TRUMP: |CLINTON: |WALLACE: )', without_brackets) 
#https://regexper.com/#%5BA-Z%5D%5BA-Z%5D%2B


language_client = language.Client()


#connect to BQ
bigquery_client = bigquery.Client(project='serge-playground')
dataset = bigquery_client.dataset('election_nlp_vegas')
#dataset.create()             

#define Schema
paragraph_id = bigquery.SchemaField('paragraph_id', 'integer')
name = bigquery.SchemaField('name', 'string')
paragraph = bigquery.SchemaField('paragraph', 'string')

sentence = bigquery.SchemaField('sentence', 'string')
sentence_id = bigquery.SchemaField('sentence_id', 'integer')
token_text = bigquery.SchemaField('token_text', 'string')
token_part_of_speach = bigquery.SchemaField('token_part_of_speach', 'string')
token_lemma = bigquery.SchemaField('token_lemma', 'string')

sentiment_polarity = bigquery.SchemaField('sentiment_polarity', 'float')
sentiment_magnitude = bigquery.SchemaField('sentiment_magnitude', 'float')

entity_name = bigquery.SchemaField('entity_name', 'string')
entity_type = bigquery.SchemaField('entity_type', 'string')
entity_salience = bigquery.SchemaField('entity_salience', 'float')
entity_wiki = bigquery.SchemaField('entity_wiki', 'string')

#create table 
table_token = dataset.table('table_token', schema=[paragraph_id, name, token_text, token_part_of_speach, token_lemma])
table_sentiment_paragraph = dataset.table('table_sentiment_paragraph', schema=[paragraph_id, name, paragraph, sentiment_polarity, sentiment_magnitude])
table_sentiment_sentence = dataset.table('table_sentiment_sentence', schema=[paragraph_id, name, sentence, sentence_id, sentiment_polarity, sentiment_magnitude])
table_entity = dataset.table('table_entity', schema=[paragraph_id, name, entity_name, entity_type, entity_salience, entity_wiki])

#table_token.create() 
#table_sentiment_paragraph.create() 
#table_sentiment_sentence.create() 
#table_entity.create() 

#insert debate into BQ

for x in range(1, len(split_for_speaker), 2):

	print(x)
	
	rows_to_insert_in_table_token = [];
	rows_to_insert_in_table_sentiment_paragraph = [];
	rows_to_insert_in_table_sentiment_sentence = [];
	rows_to_insert_in_table_entity = [];
	
	name = split_for_speaker[x]
	paragraph = split_for_speaker[x+1]
	
	document = language_client.document_from_text(paragraph);
	annotations  = document.annotate_text(include_syntax=True, include_entities=True, include_sentiment=True)
	
	for entity in annotations.entities:
		rows_to_insert_in_table_entity.append([x, name, entity.name, entity.entity_type, entity.salience, entity.wikipedia_url])
		
	for token in annotations.tokens:
		rows_to_insert_in_table_token.append([x, name, token.text_content, token.part_of_speech, token.lemma])
		
	rows_to_insert_in_table_sentiment_paragraph.append([x, name, paragraph, annotations.sentiment.polarity, annotations.sentiment.magnitude])
	
	for sentence_id, sentence in enumerate(annotations.sentences):
		document = language_client.document_from_text(sentence.content);
		sentence_annotations  = document.annotate_text(include_syntax=False, include_entities=False, include_sentiment=True)
		rows_to_insert_in_table_sentiment_sentence.append([x, name, sentence.content, sentence_id, sentence_annotations.sentiment.polarity, sentence_annotations.sentiment.magnitude])

	if len(rows_to_insert_in_table_token)>0:
		table_token.insert_data(rows_to_insert_in_table_token)
	if len(rows_to_insert_in_table_sentiment_paragraph)>0:
		table_sentiment_paragraph.insert_data(rows_to_insert_in_table_sentiment_paragraph)
	if len(rows_to_insert_in_table_sentiment_sentence)>0:
		table_sentiment_sentence.insert_data(rows_to_insert_in_table_sentiment_sentence)
	if len(rows_to_insert_in_table_entity)>0:
		table_entity.insert_data(rows_to_insert_in_table_entity)


"""
table_token = open("table_token.csv",'wb')
wr = csv.writer(table_token, dialect='excel')
for row in rows_to_insert_in_table_token:
    wr.writerow(row)
"""
	

"""https://cloud.google.com/natural-language/reference/rest/v1beta1/documents/annotateText#Token
SELECT * FROM [serge-playground:election_nlp.initial_test3] ORDER BY id LIMIT 1000 
SELECT name, AVG(LENGTH(paragraph)) FROM [serge-playground:election_nlp.initial_test] GROUP BY name
SELECT name, NTH(50, QUANTILES(LENGTH(paragraph), 101)) FROM [serge-playground:election_nlp.initial_test3] GROUP BY name
http://stackoverflow.com/questions/29092758/how-to-calculate-median-of-a-numeric-sequence-in-google-bigquery-efficiently
SELECT * FROM [serge-playground:election_nlp.initial_test3] WHERE name = 'CLINTON: ' ORDER BY id 

SELECT token_part_of_speach, COUNT(id) FROM [serge-playground:election_nlp.table_token] WHERE name='CLINTON: ' GROUP BY token_part_of_speach
SELECT token_text, COUNT(id) as count FROM [serge-playground:election_nlp.table_token] WHERE name='CLINTON: ' AND token_part_of_speach='NOUN' GROUP BY token_text ORDER BY count DESC

"""