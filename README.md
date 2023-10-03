# KafkaStreams Java
- [Introduction](#introduction)
- [Setup and run Kafka](#setup-and-run-kafka)
    - [Run Kafka cluster on local environment](#run-Kafka-cluster-on-local-environment)
    - [Run Kafka cluster on Confluent](#run-Kafka-cluster-on-confluent)
    - [Create, delete and describe Kafka topics](#create-delete-and-describe-kafka-topics)
    - [Producers and consumers using Kafka command line tools](#producers-and-consumers-using-Kafka-command-line-tools)
- [How the application works](#how-the-application-works)
    
## Introduction
This repository holds the code for experimentations on KafkaStreams technology.

To access the code, open a Terminal and start by cloning this repository with the following commands:

```
mkdir $HOME/dev
cd $HOME/dev
git clone https://github.com/robipozzi/robipozzi-kafkastreams-java
```

## Setup and run Kafka
To see how a Kafka producer works, you will need to setup a few things in advance, such as a Kafka cluster to interact with and Kafka topic to produce and consume 
messages; also it could be useful to have some Kafka consumers to test the messages have been correctly produced by our Kafka producer: everything is 
already described in details in this GitHub repository https://github.com/robipozzi/robipozzi-kafka, you will find pointers to the appropriate content in the 
paragraphs below.

### Run Kafka cluster on local environment
One option to run a Kafka cluster is obviously installing and running locally, please refer to 
https://github.com/robipozzi/robipozzi-kafka#run-Kafka-cluster-on-local-environment for all the details.

### Run Kafka cluster on Confluent
Another option to setup a Kafka cluster is to use a Cloud solution, for instance Confluent (https://www.confluent.io/), you can refer to 
https://github.com/robipozzi/robipozzi-kafka#run-Kafka-cluster-on-confluent for details regarding this option.

### Create, delete and describe Kafka topics
Once the Kafka cluster has been setup, you can find details on how to manage topics (i.e.: create, delete, ...) at 
https://github.com/robipozzi/robipozzi-kafka#create-delete-and-describe-kafka-topics

### Producers and consumers using Kafka command line tools
[TODO]

## How the application works
[TODO]# robipozzi-kafkastream-temperatures
