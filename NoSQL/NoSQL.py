import boto3
import csv 

# Define the name of the bucket to create
bucket_name = "hw3-s3-bucket"

# Have the access key and secret key stored in a local, unpublished file
#  if needed, can be given to the grader on request
with open('aws_credentials.txt', 'r') as f:
	access_key_id = f.readline().rstrip()
	secret_access_key = f.readline().rstrip()

# Initialize an S3 instance
s3 = boto3.resource('s3', 
	aws_access_key_id=access_key_id, 
	aws_secret_access_key=secret_access_key 
) 

# Create a new bucket
try:
	s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}) 
except Exception as e: 
	print(e)

bucket = s3.Bucket(bucket_name)

# Create a DynamoDB table
dyndb = boto3.resource('dynamodb', 
	region_name='us-west-2', 
	aws_access_key_id=access_key_id, 
	aws_secret_access_key=secret_access_key
)

# Define the schema and keys for the database
try: 
	table = dyndb.create_table( 
		TableName='DataTable', 
		KeySchema=[ 
			{ 
				'AttributeName': 'PartitionKey', 
				'KeyType': 'HASH' 
			}, 
			{ 
				'AttributeName': 'RowKey', 
				'KeyType': 'RANGE' 
			}
		], 
		AttributeDefinitions=[ 
			{ 
				'AttributeName': 'PartitionKey', 
				'AttributeType': 'S' 
			}, 
			{ 
				'AttributeName': 'RowKey', 
				'AttributeType': 'S' 
			}
		], 
		ProvisionedThroughput={ 
			'ReadCapacityUnits': 5, 
			'WriteCapacityUnits': 5 
		} 
	) 
except Exception as e: 
	table = dyndb.Table("DataTable")

# Wait for the table to be created
table.meta.client.get_waiter('table_exists').wait(TableName='DataTable') 

# Structure the data on the cloud
with open(r'experiments.csv', 'r', encoding="utf-8-sig") as csvfile: 
	csvf = csv.DictReader(csvfile, delimiter=',')
	for row in csvf: 
		# Add each data object (experiment file) into a blob store on S3
		body = open(row['URL'], 'rb') 
		s3.Object(bucket_name, row['URL']).put(Body=body) 
		md = s3.Object(bucket_name, row['URL']).Acl().put(ACL='public-read') 

		# Use the URL of the blob for location in the database
		url = 'https://s3-us-west-2.amazonaws.com/' + bucket_name + '/' + row['URL']
		
		# Create a metadata item as a row for the database
		metadata_item = {
			'PartitionKey': row['Id'],
			'RowKey': row['Id'],
			'Temp' : row['Temp'], 
			'Conductivity' : row['Conductivity'], 
			'Concentration' : row['Concentration'],
			'url': url
		}

		# Enter that metadata row into the table
		table.put_item(Item=metadata_item)