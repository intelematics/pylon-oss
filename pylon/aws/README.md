We provide a number of wrappers for key AWS services.

## Contents

* [S3](#S3)
	* [getObject](#S3.getObject)
	* [putObject](#S3.putObject)
	* [getPath](#S3.getPath)
	* [splitPath](#S3.splitPath)
	* [Bucket](#S3.Bucket)
		* [put](#S3.Bucket.put)
		* [get](#S3.Bucket.get)
		* [list](#S3.Bucket.list)
		* [delete](#S3.Bucket.delete)
* [SQS](#SQS)
	* [Queue](#SQS.Queue)
		* [sendMessage](#SQS.Queue.sendMessage)
		* [sendMessages](#SQS.Queue.sendMessages)
		* [getMessage](#SQS.Queue.getMessage)
		* [getMessages](#SQS.Queue.getMessages)
		* [len](#SQS.Queue.len)
	* [Novel use cases](#SQS.Novel)
		* [Transfer all messages](#SQS.Novel.transfer)
		* [Easily add messages](#SQS.Novel.add)
* [SNS](#SNS)
	* [Topic](#SNS.Topic)
		* [sendMessage](#SNS.Topic.sendMessage)
* [Lambda](#Lambda)
	* [Function](#Lambda.Function)
 	* [Novel use cases](#Lambda.Novel)
	 	* [Batch S3 Operations](#Lambda.Novel.batch)
* [DynamoDB](#DynamoDB)
	* [Table](#DynamoDB.Table)
		* [get](#DynamoDB.Table.get)
		* [put](#DynamoDB.Table.put)
		* [delete](#DynamoDB.Table.delete)
		* [fullScan](#DynamoDB.Table.fullScan)
* [RDS](#RDS)
	* [DatabaseConnection](#RDS.DatabaseConnection)
		* [tableExists](#RDS.DatabaseConnection.tableExists)
		* [insertOrIgnore](#RDS.DatabaseConnection.insertOrIgnore)
		* [fetch](#RDS.DatabaseConnection.fetch)
* [SSM](#SSM)
	* [ParameterStore](#SSM.ParameterStore)
		* [get](#SSM.ParameterStore.get)

# <a name="S3"></a>S3

* ### <a name="S3.getObject"></a> `getObject(s3Path, encoding=None)`

	Retrieves the content of the object at the given S3 path.
		
	Example:
		
	```python
	>>> pylon.aws.s3.getObject('s3://my-bucket/a-key/my-json-file.json', encoding='utf-8')
	'{"this":"is", "some":"very", "cool":"json"}'
	```
* ### <a name="S3.putObject"></a> `putObject(s3Path, content, encoding=None, metadata: dict=None)`

	Puts the content into an object at the given S3 path. Metadata dictionary is set as the S3 object metadata.
		
	Example:
		
	```python
	jsonText = '{"this":"is", "some":"very", "cool":"json"}'
	pylon.aws.s3.putObject(
		's3://my-bucket/a-key/my-json-file.json',
		jsonText,
		encoding='utf-8',
		metadata={'isVeryCool':True}
	)
	```

* ### <a name="S3.getPath"></a> `getPath(bucketName, key)`

	Converts a given bucketName and key into a fully qualified S3 path.
		
	Example:
		
	```python
	>>> pylon.aws.s3.getPath('my-bucket', 'a-key/my-json-file.json')
	's3://my-bucket/a-key/my-json-file.json'
	```

* ### <a name="S3.splitPath"></a> `splitPath(s3Path: str)`

	Splits a fully qualified S3 path into a bucket name and key.
		
	Example:
		
	```python
	>>> pylon.aws.s3.splitPath('s3://my-bucket/a-key/my-json-file.json')
	('my-bucket', 'a-key/my-json-file.json')
	```

## <a name="S3.Bucket"></a> Bucket
We also provide a wrapper class for an S3 bucket.

Initialise it using the name of the bucket

```python
bucket = pylon.aws.s3.Bucket('my-bucket')
```

There are a number of basic bucket operations in this class.

* #### <a name="S3.Bucket.put"></a> `put(self, key: str, content: _STR_OR_BYTES, encoding: str=None, metadata: dict=None)`

	Puts the content into the bucket at the given key. Metadata dictionary is set as the S3 object metadata.
	
	Example:
	
	```python
	jsonText = '{"this":"is", "some":"very", "cool":"json"}'
	bucket.put(
		'a-key/my-json-file.json',
		jsonText,
		encoding='utf-8',
		metadata={'isVeryCool':True}
	)
	```
	
* #### <a name="S3.Bucket.get"></a> `get(self, key, encoding=None)`

	Retrieves the content of the object at the key in the bucket.
	
	Example:
	
	```python
	>>> bucket.get('a-key/my-json-file.json', encoding='utf-8')
	'{"this":"is", "some":"very", "cool":"json"}'
	```
	
* #### <a name="S3.Bucket.list"></a> `list(self, prefix: str=None)`

	Efficiently lists all keys in the bucket filtered by a common prefix.
	
	Example:
	
	```python
	>>> bucket.list(prefix='a-key')
	['a-key/my-json-file.json', 'a-key/another-json-file.json', 'a-key/something-else.txt']
	```
	
* #### <a name="S3.Bucket.delete"></a> `delete(self, keys: typing.List[str])`

	Deletes all objects at the keys in the list.
	
	Example:
	
	```python
	keys = ['a-key/my-json-file.json', 'a-key/another-json-file.json', 'a-key/something-else.txt']
	bucket.delete(keys)
	```

# <a name="SQS"></a> SQS

## <a name="SQS.Queue"></a> Queue

We provide a wrapper class for an SQS Queue. This class conforms to the `MessageConsumer` and `MessageProducer` protocols.

Initialise it using the name of the queue

```python
queue = pylon.aws.sqs.Queue('my-queue')
```

This class provides methods for sending and receiving messages.

**NB: This wrapper only supports pylon format messages (ie. subclasses of `pylon.models.messages.BaseMessage`). In the future it would be nice to support true raw messages.**

```python
import pylon.models.messages as messages
	
message                 = messages.BaseMessage()
message.body            = 'very cool message'
message.payloadType     = messages.PayloadType.INLINE
message.payloadMimeType = 'text'
message.objectType      = messages.ObjectType.RAW_CONTENT
```

* #### <a name="SQS.Queue.sendMessage"></a> `sendMessage(self, message: BaseMessage)`

	Appends a single message to the queue.
	
	Example:
	
	```python
	queue.sendMessage(message)
	```

* #### <a name="SQS.Queue.sendMessages"></a> `sendMessages(self, messages: typing.Iterable[BaseMessage])`

	Appends a list of messages to the queue. The underlying `boto3` API only allows batches of 10 messages to be sent at a time. However pylon will automatically batch them. `messages` can be any type of iterable including a generator.
	
	Example:
	
	```python
	def messageGenerator(nMessages):
		for i in range(nMessages):
			message                 = messages.BaseMessage()
			message.body            = f'Message number {i}'
			message.payloadType     = messages.PayloadType.INLINE
			message.payloadMimeType = 'text'
			message.objectType      = messages.ObjectType.RAW_CONTENT
			yield message
			
	queue.sendMessages(messageGenerator(200))
	```

* #### <a name="SQS.Queue.getMessage"></a> `getMessage(self)`

	Receives a single message from the front of the queue. This method uses the `contextmanager` pattern. The message is automatically removed from the queue unless the `with` block is exited via an Exception.
	
	Example:
	
	```python
	with queue.getMessage() as message:
		print(message.body)
	```

* #### <a name="SQS.Queue.getMessages"></a> `getMessages(self, maxMessages)`

	Receives up to `maxMessages` messages from the front of the queue. This method uses the `contextmanager` pattern. The messages are first passed to the `with` block and then they are all deleted at once when the block exits naturally. If an exception is raised then none of the messages are deleted.
	
	Example:
	
	```python
	with queue.getMessages(10) as messages:
		for message in messages:
			print(message.body)
	```

* #### <a name="SQS.Queue.len"></a> `__len__(self)`

	The `__len__` builtin function is overridden to provide the ***approximate*** number of messages in the queue.
	
	Example:
	
	```python
	>>> len(queue)
	1234
	```
	
## <a name="SQS.Novel"></a> Some novel use cases

* #### <a name="SQS.Novel.transfer"></a> Transfer all messages from one queue to another

	For example when you want to feed messages from the dead letter queue back into the main queue
	
	```python
	# Use 2x length to make sure we catch all messages
	nMessages = 2 * len(deadLetterQueue)
	with deadLetterQueue.getMessages(nMessages) as allMessages:
		mainQueue.sendMessages(allMessages)
	```
* #### <a name="SQS.Novel.add"></a> Easily add messages to a queue from the command line

	```python
	import pylon

	queue = pylon.aws.sqs.Queue('pylon_demo_in')

	def sendMessage(body):
	    message                  = pylon.models.messages.BaseMessage()
	    message.body             = body
	    message.payloadType      = pylon.models.messages.PayloadType.INLINE
	    message.payloadMimeType  = 'text'
	    message.objectType       = pylon.models.messages.ObjectType.RAW_CONTENT
	    queue.sendMessage(message)

	if __name__ == '__main__':
	    while True:
	        sendMessage(input('Body: '))
	```

# <a name="SNS"></a> SNS

## <a name="SNS.Topic"></a> Topic

We provide a wrapper class for an SNS Topic. This class conforms to the `MessageConsumer` protocol.

Initialise it using the ARN of the topic.

```python
topic = pylon.aws.sns.Topic('arn:aws:sns:ap-southeast-2:123456789012:my_topic')
```

This class provides methods for posting messages as notifications.

**NB: This wrapper only supports pylon format messages (ie. subclasses of `pylon.models.messages.BaseMessage`). In the future it would be nice to support true raw messages.**

```python
import pylon.models.messages as messages
	
message                 = messages.BaseMessage()
message.body            = 'very cool message'
message.payloadType     = messages.PayloadType.INLINE
message.payloadMimeType = 'text'
message.objectType      = messages.ObjectType.RAW_CONTENT
```

* #### <a name="SNS.Topic.sendMessage"></a> `sendMessage(self, message: BaseMessage)`

	Post a single message as a notification
	
	Example:
	
	```python
	topic.sendMessage(message)
	```

# <a name="Lambda"></a> Lambda

## <a name="Lambda.Function"></a> Function

We provide a wrapper class for Lambda functions. Instances of this class are callable meaning they behave exactly like python functions. All lambda functions take a dictionary as the input. Initialise the function with it's name. By default functions are called synchronously. Synchronous function calls will return the output of the lambda function as a string. If you would like the lambda to execute asynchronously you can specify `asynchronous=True` on initialisation. However, asynchronous calls return immediately so you will not receive any result from the lambda.

#### Synchronous
```python
>>> myLambdaFunction = pylon.aws.lambda_.Function('my-lambda-function')
>>> myLambdaFunction({'some':'input'})
'{"some":"output"}'
```

#### Asynchronous
```python
>>> myLambdaFunction = pylon.aws.lambda_.Function('my-lambda-function', asynchronous=True)
>>> myLambdaFunction({'some':'input'})
```

## <a name="Lambda.Novel"></a> Some novel use cases

* #### <a name="Lambda.Novel.batch"></a> Batch S3 Operations

	Say you want to perform a lambda function on each file in a bucket in parallel but don't want to deal with the hassel of setting up an S3 Batch Job
	
	```python
	import pylon
	
	# Initialise the bucket and the lambda function.
	# The function must be set to asynchronous execution because we want the
	# tasks to run in parallel.
	bucket = pylon.aws.s3.Bucket('my-bucket')
	task = pylon.aws.lambda_.Function('my-single-file-task', asynchronous=True)
	
	# Get the list of raw keys
	keys = bucket.list(prefix='some-key-prefix')
	
	# Transform the raw keys into lambda function inputs
	keys = ({'key':k['Key']} for k in keys)
	
	# Call the lambda once for every key
	for key in keys:
		task(key)
	```

# <a name="DynamoDB"></a> DynamoDB

## <a name="DynamoDB.Table"></a> Table

We provide a wrapper class for a DynamoDb table. This class provides basic operations for setting and getting records in the table. Initialise an instance of this class using the name of the table.

```python
table = pylon.aws.dynamodb.Table('my-table')
```

* #### <a name="DynamoDB.Table.get"></a> `get(self, key: dict)`

	Retrieves an item from the table which is uniquely identified by the fields in the given key dictionary.
	
	Example:
	
	```python
	>>> table.get({'id': 50})
	{'id':50, 'some':'other', 'cool':'values', 'ttl':1234567890}
	```

* #### <a name="DynamoDB.Table.put"></a> `put(self, item: dict)`

	Puts the given item into the table.
	
	Example:
	
	```python
	value = {'id':50, 'some':'other', 'cool':'values', 'ttl':1234567890}
	table.put(value)
	```

* #### <a name="DynamoDB.Table.delete"></a> `delete(self, key: dict)`

	Deletes an item from the table which is uniquely identified by the fields in the given key dictionary.
	
	Example:
	
	```python
	table.delete({'id': 50})
	```

* #### <a name="DynamoDB.Table.fullScan"></a> `fullScan(self)`

	Fetches every item from the table.
	
	Example:
	
	```python
	>>> table.fullScan()
	[{'id':50, 'some':'other', 'cool':'values', 'ttl':1234567890},
	{'id':51, 'some':'a', 'cool':'b', 'ttl':1234567890},
	{'id':52, 'some':'c', 'cool':'d', 'ttl':1234567890},
	{'id':53, 'some':'e', 'cool':'f', 'ttl':1234567890}]
	```

# <a name="RDS"></a> RDS

## <a name="RDS.DatabaseConnection"></a> DatabaseConnection

We provide a wrapper class for RDS database connections. This class provides some basic database operations as methods. Initialse an instance of this class by providing the `host`, `username`, and `password` of the database. By default this class assumes the database is `MySQL` but if you are connecting to a different database type you can optionally specify the `protocol` and `driver`. This class builds on `SQLAlchemy` so the `protocol` and `driver` values should be the same as you would use for that.

Default:

```python
database = pylon.aws.rds.DatabaseConnection('my-host', 'my-user', 'my-pass')
```

Specific protocol:

```python
database = pylon.aws.rds.DatabaseConnection('my-host', 'my-user', 'my-pass', protocol='mysql', driver='pymysql')
```

* #### <a name="RDS.DatabaseConnection.tableExists"></a> `tableExists(self, schema: str, tableName: str)`

	Returns a boolean value whether the given table exists in the given schema.
	
	Example:
	
	```python
	>>> database.tableExists('my-schema', 'table-that-exists')
	True
	>>> database.tableExists('my-schema', 'table-that-does-not-exist')
	False
	```

* #### <a name="RDS.DatabaseConnection.insertOrIgnore"></a> `insertOrIgnore(self, schema: str, tableName: str, records: typing.Iterable[dict])`

	Attempts to insert the given records into the given schema and table. If there is already a record with the same primary key(s) the new record is ignored. When inserting many records where some are new and some are pre-existing, all the new records will be inserted and the pre-existing records will be skipped.
	
	Example:
	
	```python
	records = [{'id':1, 'first-field':'hi', 'second-field':'hello'},
	{'id':1, 'first-field':'yo', 'second-field':'sup'}]
	database.insertOrIgnore('my-schema', 'table-that-exists', records)
	```

* #### <a name="RDS.DatabaseConnection.fetch"></a> `fetch(self, schema: str, tableName: str, where: typing.List[dict]=None)`

	Fetches records from the given schema and table filtered by the given where clauses. Where clauses are combined with `AND`, and the operators supported are named per the stdlib `operator` module. The where clauses should be formatted as follows:
	
	```python
	{'key': 'some-field-name', 'op': 'eq', 'value': 'some-value'}
	```
	where `key` is the name of the field in the database, `op` is the operator to use, and `value` is the value to compare against.
	
	Example:
	
	```python
	>>> database.fetch(
			schema='my-schema',
			tableName='table-that-exists',
			where=[
				{'key': 'some-foreign-key', 'op': 'eq', 'value': 'abcd'},
				{'key': 'dimUTCTimestamp', 'op': 'ge', 'value': 123456789}
			]
		)
	[{'id':1, 'some-foreign-key':'abcd', 'dimUTCTimestamp': 123456789, 'another-field':'hello"},
	{'id':2, 'some-foreign-key':'abcd', 'dimUTCTimestamp': 123456790, 'another-field':'hi"}
	{'id':3, 'some-foreign-key':'abcd', 'dimUTCTimestamp': 123456791, 'another-field':'yo"}
	{'id':4, 'some-foreign-key':'abcd', 'dimUTCTimestamp': 123456792, 'another-field':'sup"}]
	```

# <a name="SSM"></a> SSM

## <a name="SSM.ParameterStore"></a> ParameterStore

We provide a wrapper class for fetching values from Parameter Store. This is a very light class so only has limited functionality. There are no instance methods in this class so no need to initialise it.

* #### <a name="SSM.ParameterStore.get"></a> `get(cls, key: str)`

	Fetches the parameter with the given key from Parameter Store.
	
	Example:
	
	```python
	>>> pylon.aws.ParameterStore.get('some-key')
	'whoa this is a cool value'
	```
