This is the start of something incredible

## Contents

* [Introduction](#Introduction)
	* [Features](#Introduction.Features)
	* [What is a pipeline component?](#Introduction.whatis)
		* [The input queue](#Introduction.whatis.input)
		* [The compute](#Introduction.whatis.compute)
		* [The output topic](#Introduction.whatis.output)
* [Models](#Models)
	* [Messages](#Models.Messages)
		* [Message schema](#Models.Messages.schema)
	* [Data Assets](#Models.DataAssets)
	* [Ingestion Steps](#Models.IngestionSteps)
* [Usage](#Usage)
	* [Installation](#Usage.Installation)
	* [Pipeline Components](#Usage.Components)
		* [Pipeline Component](#Usage.Components.Pipeline)
		* [Other Components](#Usage.Components.Other)
	* [Configuration](#Usage.Configuration)
		* [Task Config](#Usage.Configuration.TaskConfig)
		* [Environment](#Usage.Configuration.Environment)
		* [Dockerfile](#Usage.Configuration.Dockerfile)
		* [Docker Compose](#Usage.Configuration.DockerCompose)
		* [Lambda](#Usage.Configuration.Lambda)
	* [Composition](#Usage.Composition)
	* [Logging](#Usage.Logging)
		* [Heartbeats](#Usage.Logging.Heartbeats)
		* [Performance](#Usage.Logging.Performance)
* [Testing](#Testing)
	* [Running tests](#Testing.running)

# <a name="Introduction"></a> Introduction

Pylon is a python framework for developing pipeline components. Using standardised models we make it easy to create new components, hook up to existing components, and access several AWS serivces.

## <a name="Introduction.Features"></a> Features

* [Standardised pipeline component model](#Introduction.whatis)
* [Standardised messaging format and protocol](#Models.Messages)
* [Deploy the exact same code to AWS ECS or Lambda](#Usage.Components.Pipeline)
* [Pipeline components are easily composable](#Usage.Composition)
* [Automatic S3 message retrieval](#Models.Messages.S3Retrieval) (and storage coming soon<sup>TM</sup>)
* [Automatic heartbeat logging](#Usage.Logging.Heartbeats)
* [Automatic performance logging](#Usage.Logging.Performance)
* [Automatic data lineage record keeping](#Models.IngestionSteps)
* [Many AWS product wrappers](pylon/aws/README.md)
	* DynamoDB
	* Lambda
	* S3
	* SNS
	* SQS
	* SSM
	* RDS
	* and more as we need them

## <a name="Introduction.whatis"></a> What is a pipeline component?

Through multiple design iterations we came to the conclusion that we can have an atomic unit for building data pipelines. We call this atomic unit a pipeline component. The basic design of a pipeline component is as follows:

![The Dream](the_dream_diagram.png)

A pipeline component is composed of the elements inside the orange dotted line. It consists of:

* A standardised input queue
* Some scalable compute
* A standardised output topic

#### <a name="Introduction.whatis.input"></a> The input queue

The input queue is subscribed to one or many topics. These subscriptions can have attribute filters if the component in question is only interested in particular types of messages.

Alternatively messages can be pushed directly into the queue manually or by some other service.

The queue stores the messages until something (the compute) consumes the messages.

#### <a name="Introduction.whatis.compute"></a> The compute

The compute for the component will recieve a single message from the front of the input queue and perform some operations on it. Once it has completed the operations it will return the result(s) to be published by the output topic. Then it will pick up the next message from the front of the queue. Ideally these operations are as close to being pure functions as possible. However, in practice this is often not possible.

Currently pylon supports the following forms of compute:

* AWS Lambda
* AWS ECS (Elastic Compute Service)
* Anything that can run a docker container and has an internet connection

#### <a name="Introduction.whatis.output"></a> The output topic

The output topic is used as a hook for other components to consume the output of this component. When messages are published by the topic any subscribers receive the message. The topic itself does not store any message.

# <a name="Models"></a> Models

## <a name="Models.Messages"></a> Messages

Pylon defines a standardised message schema. Pylons wrappers of AWS queues and topics will automatically serialize and deserialze messages of this schema to and from message model objects.

<a name="Models.Messages.S3Retrieval"></a>SQS and SNS both have a message size limit of 256kb. If the message size is approaching this limit you should store the message body in S3 and set the `payloadS3Path` attribute on the message. Pylon will automatically retrieve the body from S3.

**NB: We would also like to automatically store the message body. We believe the storage location should be a property of the queue or topic that the message is being published to. The plan was to use tags to associate the storage location with the queue or topic. However, SNS topics do not currently support tags so we will have to wait until they do before we can implement this feature.**

#### <a name="Models.Messages.schema"></a> Message schema

The base pylon message class is `pylon.models.messages.BaseMessage`. All other message types subclass this base class. `BaseMessage` has the following attributes:

* #### `body` (required)

	The main body of the message. This can be any string or subclass of `pylon.interfaces.serializing.JsonSerializable`.

* #### `payloadType` (required)

	The location of the message body. There are two valid options which are found in `pylon.models.messages.PayloadType`

	* `PayloadType.S3`

		The body of the message is stored in S3. Pylon will automatically retrieve the message body from S3 upon deserialization using the location in the `payloadS3Path` attribute.

	* `PayloadType.INLINE`

		The body of the message is stored in the body attribute.

* #### `payloadMimeType` (required)

	The encoding of the message body.

	Example:

	```
	text/json
	```

* #### `objectType` (required)

	The type of object in the body. This is used for topic subscription filtering. There are several valid options which are found in `pylon.models.messages.ObjectType`:

	* `ObjectType.DATA_ASSET`

		The body is a data asset object

	* `ObjectType.INGESTION_STEP`

		The body is an ingestion step	object

	* `ObjectType.URL_RESOURCE`

		The body is a JSON string used to record the fact that we fetched data from a remote data source. This has it's own defined schema.

		```json
		{
		    "destination": "s3://<my-bucket>/<key-to-destination>/the-file.json",
		    "resource": "https://somedomain.com/a/very/cool/datasource.json",
		    "metadata": {
		        "size": 12345,
		        "timestamp": "2019-05-07T22:13:33+00:00"
		    },
		    "auth": {
		        "user": "user",
		        "pass": "very-secure"
		    }
		}
		```

	* `ObjectType.UPDATE_NOTIFICATION`

		The body is a JSON string representing a notification that some data asset has been updated. This has it's own defined schema.

		```json
		{
		    "dataAssetName": "schema.someDataAsset",
		    "dimUTCTimestamp": 1557258667,
		    "recordsUpdated": 123
		}
		```

	* `ObjectType.RAW_CONTENT`

		The body is any arbitrary string.

	* `ObjectType.NULL`

		The body contains no information.

	* `ObjectType.LAMBDA_EVENT`

		The body is the event for a lambda invocation.


* #### `ingestionId` (required)

	The unique identifier for the ingestion step that produced this message. Pylon will automatically set this value if the message is produced by a Pipeline Component.

* #### `payloadS3Path` (optional)

	If the `payloadType` is `S3` then this attribute is set to the S3 path of the body. Should be a fully qualified S3 path of the form `s3://<my-bucket>/<some-key>/.../the-message-body.json`

* #### `dataAssetName` (optional)

	If the `objectType` is `DATA_ASSET` then this is set to the name of the data asset.

* #### `dataAssetCountry` (optional)

	If the `objectType` is `DATA_ASSET` then this is set to the country of the data asset.

## <a name="Models.DataAssets"></a> Data Assets

Pylon provides a model class that represents data assets. It contains everything needed to store a data asset in our data lake. This model class inherits `JsonSerializable` so it will be automatically serialized and deserialized to and from JSON when an instance of this class is set as the body of a pylon message.

The data asset model has the following attributes:

* #### `dataAssetName`

	The name of the data asset. This must include the schema as well as the actual entity/feature name.

	Example:

	```
	incident.entityIncident
	```

	Where `incident` is the schema and `entityIncident` is the entity name.

* #### `dataAssetCountry`

	The two letter code of the country the data originated in.

	Example:

	```
	AU
	```

* #### `dataAssetPartitionKeys`

	Ordered list of data asset fields by which the data is partitioned. These keys are specified in the data definition for the data asset.

* #### `dataAssetUniqueKeys`

	Unordered list of data asset fields used for deduplication. Two data asset records are considered identical if their values for each of these keys match. These keys are specified in the data definition for the data asset.

* #### `data`

	List of dictionaries mapping the data asset fields to their values.

* #### `ingestionId`

	The unique identifier of the ingestion step that produced this record.

## <a name="Models.IngestionSteps"></a> Ingestion Steps

Pylon includes a model for record of ingestion steps that occurred. Each instance of this model class records information about a single execution of a single component. This is useful for data lineage. Every record in the data late has an `ingestionID` value which will allow us to retrieve information about the exact component execution that produced the record. Each ingestion step also records the `ingestionID` of the step that came before. This allows us to trace the chain of transformations all the way back to an initial ingestion from the root data source.

If you are developing components you will never need to interact with these model objects directly as pylon handles **everything** automatically.

Ingestion steps have the following attributes:

* #### `ingestionId`

	The unique identifier of this ingesion step.

* #### `parentIngestionId`

	The unique identifier of the ingestion step that preceded this one. `None` if there is no such step.

* #### `artifactName`

	The name of the component the executed this ingestion step.

* #### `artifactVersion`

	The version of the component that executed this ingestion step.

* #### `dimUTCTimestamp`

	The time of execution of this ingestion step.

* #### `dimUTCDateId`

	The date of execution of this ingestion step.

* #### `dimUTCHour`

	The hour of execution of this ingestion step.

* #### `metadata`

	A dictionary containing arbitrary metadata about this ingestion step. If the component returns a metadata dictionary in the `PylonResult` then it will be automatically populated into this dictionary.

# <a name="Usage"></a> Usage

## <a name="Usage.Installation"></a> Installation

To install pylon on your local machine `cd` to the pylon directory and run

```bash
$ make dev
```

## <a name="Usage.Components"></a> Pipeline Components

#### <a name="Usage.Components.Pipeline"></a> Pipeline Component

Pylon is designed to make it easy to develop pipeline components. Since a pipeline component is essentially a function with standardised input and output we are able to provide a python decorator to abstract away all of the I/O. To write a pipeline component simply use the `@pylon.PipelineComponent` decorator on your work function. This function will be executed once per message. Your function is expected to take 2 arguments. A message, and a config dictionary. In order to pass the result to the output topic, this function is expected to return a list of `BaseMessage` subclasses wrapped in a `PylonResult` instance. Although it is possible your function may sometimes not return any messages. You may also optionally return an arbitrary dictionary of metadata. We have provided the `pylon.result()` function to assist with the wrapping.

```python
import pylon

@pylon.PipelineComponent
def pipelineTask(message, config):
	message.body = 'Hello pylon!'
	metadata = {'task': 'example transform task'}
	return pylon.result([message], metadata)
```

We still need to tell pylon to actually execute it. We typically do this using the `runForever()` function on the `pylon.PipelineComponent` decorator class. Optionally you could also call `runOnce()` if you only wanted to process a single message. We would normally call `runForever()` from within the standard script entrypoint.

```python
if __name__ == '__main__':
	pipelineTask.runForever()
```

In order to allow this pipeline component to also run in lambda we must define a lambda entrypoint function. Lambda functions are triggered with 2 arguments `event` and `context`. When lambda is triggered by SQS the `event` contains one or more messages. We provide a `lambda_handler` function on the `PipelineComponent` decorator class to separate the messages from the event. **NB: The entire batch will be deleted at once only if all messages in the batch are successfully processed.**

```python
def lambda_entrypoint(event, context):
	pipelineTask.lambda_handler(event, context)
```

Putting it all together we have the entire example pipeline component that accepts messages from an input queue and publishes the result to an output topic. The exact same code can be deployed to either ECS or lambda.

```python
import pylon

@pylon.PipelineComponent
def pipelineTask(message, config):
	message.body = 'Hello pylon!'
	metadata = {'task': 'example transform task'}
	return pylon.result([message], metadata)

def lambda_entrypoint(event, context):
	pipelineTask.lambda_handler(event, context)

if __name__ == '__main__':
	pipelineTask.runForever()
```

#### <a name="Usage.Components.Other"></a> Other Component Types

In addition to the pipeline component pylon also has two other types of component. To use either of these component types simply change the decorator on the component task. All else should remain the same.

* `pylon.SourceComponent`

	A source component is a component at the start of a pipeline. Source components do not have an input queue and so the `message` argument in the task function is always `None`. Examples of source components are things like tasks which fetch data from an external source rather than having it passed to them by a queue.

* `pylon.SinkComponent`

	A sink component is a component at the end of a pipeline. Sink components do not have an output topic so it has nowhere to deliver any output messages. Sink components should not be part of a pipeline or data transformation in and of themselves. As such they do not even produce ingestion step records. Examples of sink components include the RDS Writer and the Data Asset Writer which simply write the recieved data asset message to a data store without modifying it.

## <a name="Usage.Configuration"></a> Configuration

#### <a name="Usage.Configuration.TaskConfig"></a> Task Config

Pipeline components must have a value set for the `PYLON_CONFIG` environment variable. For docker based deployments specify this on the container. For lambda deployments set the lambda environment variable.

The value for `PYLON_CONFIG` is expected to be either a JSON dictionary or the location of a Parameter Store parameter containing the JSON dictionary. If it is a Parameter Store parameter pylon will automatically fetch this value.

Any values set in the `PYLON_CONFIG` dictionary will be passed to the component task via the `config` argument.

There are 2 mandatory keys to be set in `PYLON_CONFIG` depending on the component type

* #### `INPUT_QUEUE_NAME`

	The name of the queue used as the input for the component. This key is mandatory for `pylon.PipelineComponent` and `pylon.SinkComponent` tasks.

* #### `OUTPUT_TOPIC_ARN`

	The ARN of the topic used as the output for the component. This key is mandatory for `pylon.PipelineComponent` and `pylon.SourceComponent` tasks.

#### <a name="Usage.Configuration.Environment"></a> Environment Variables

It is also recommended to provide a `.env` file containing a list of environment variables to be set in the execution container. These values provide additional information about the component but are not absolutely critical to it's execution. Please provide these values whenever possible as they are used for identifying the component in the ingestion steps and API calls.

Pylon will automatically load this file and add it to the config at runtime so your component has access to these values from inside the task function via the `config` dictionary.

Example:

```
IMAGE_NAME=pylon_example
ECR_URL=352735350688.dkr.ecr.ap-southeast-2.amazonaws.com
MAJOR=0
MINOR=5
PATCH=17
```

These five values are the recommended ones. However you may add any arbitrary additional values to be loaded into the `config` dictionary at runtime.

**NB: Any values set in `.env` will overwrite values set in `PYLON_CONFIG`**

#### <a name="Usage.Configuration.Dockerfile"></a> Dockerfile

To build a docker image with pylon you simply need to build on top of the base pylon image.

Example:

```
FROM 352735350688.dkr.ecr.ap-southeast-2.amazonaws.com/pylon:latest
WORKDIR /opt/app

ADD .env .env
ADD example.py .

ENTRYPOINT [ "python", "example.py" ]
```

Where `example.py` is the file containing the code for your component and `.env` is the file with your additional environment variables.

#### <a name="Usage.Configuration.DockerCompose"></a> Docker Compose

To run the component on your local machine you'll need a `docker-compose.yaml`. Set the `PYLON_CONFIG` as an environment variable for the container and map your valid AWS credentials into the appropriate directory inside the container.

Example:

```yaml
version: '3'

services:

  component:
    build:
      dockerfile: Dockerfile
      context: .
    image: ${ECR_URL}/${IMAGE_NAME}:latest
    environment:
      PYLON_CONFIG: >-
        {
          "INPUT_QUEUE_NAME": "my-queue",
          "OUTPUT_TOPIC_ARN": "arn:aws:sns:ap-southeast-2:123456789012:my_topic",
          "SOMETHING_ELSE": "WHOA A COOL EXTRA VALUE"
        }
    volumes:
      - ~/.aws:/root/.aws
```

#### <a name="Usage.Configuration.Lambda"></a> Lambda

In order to use pylon in a lambda function you must add the pylon layer to the lambda function. A Lambda Layer is published with every release of pylon.

## <a name="Usage.Composition"></a> Composition

Components are designed to be easily composable into full data processing pipelines. This is achieved using SQS to SNS subscriptions. To hook up the output of one component to the input of another add a subscription from the input queue to the output topic. It is possible to connect multiple input queues from different components to the same output topic if you require a fan-out design.

You can also use subscription filters to only receive a subset of the published messages in the queue. For example, if you're only interested in data asset messages in a given component, you would filter to only receive messages where the message attribute `objectType` is set to `dataAsset`. This kind of filtering is very useful for achieving separation of concerns. You need only build a component to handle a single type of message and only filter for that type. Usually you would want to filter the messages as all components (except `SinkComponent`) will output an ingestion step.

Another useful design is having the same component input queue subscribed to multiple output topics simulaneously. This is very useful for component reusability. For example, the RDS Writer component subscribes to the end of every pipeline and filters for data asset messages. Thus we only have a single component responsible for writing to RDS which is great for maintainability. It's very microservicey.

**NB: Make sure you set "Raw message delivery" to `true` in the subscription attributes. Otherwise AWS will mangle the message when passing it from SNS to SQS.**

**NB: Make sure you add a policy to the SNS topic giving it permission to write to the SQS queues that are subscribed to it.**

## <a name="Usage.Logging"></a> Logging

For all logging we use the python `logging` package. Pylon automatically sets it up with a base config.

#### <a name="Usage.Logging.Heartbeats"></a> Heartbeats

Pylon will automatically log a heartbeat whenever a component is started including information about the execution type.

In addition to this it will also log a heartbeat whenever a message is processed.

To use these logs as a metric use the following metric filter:

Whenever a message is processed

```
heartbeat core_process
```

Whenever the component is started for a single execution

```
heartbeat run_once
```

Whenever the component is started as an infinite loop

```
heartbeat run_forever
```

Whenever the component is started as a lambda

```
heartbeat lambda_handler
```

#### <a name="Usage.Logging.Performance"></a> Performance

Pylon will automatically time the execution of the component task and log the execution time. It times both the execution time with I/O and without I/O. In order to use these logs as a cloudwatch metric use the following metric filter:

To get the time without I/O

```
[..., metric=timed_execution, name=core_process, time, ]
```

To get the time with I/O

```
[..., metric=timed_execution, name=total, time, ]
```

# <a name="Testing"></a> Testing

## <a name="Testing.running"></a> Running tests
1. If not already installed, install `pytest` and `pytest-cov` using your favourite python package manager
2. Ensure you have run `python setup.py develop` previously
3. From the root directory of the repository run `pytest`

