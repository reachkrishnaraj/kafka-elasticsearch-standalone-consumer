# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

## Architecture of the kafka-elasticsearch-standalone-consumer [indexer]

![](https://raw.githubusercontent.com/ppine7/kafka-elasticsearch-standalone-consumer/master/img/IndexerV2Design.jpg)


# Introduction

### **Kafka Standalone Consumer [Indexer] will read messages from Kafka, in batches, process and bulk-index them into ElasticSearch.**

### _As described in the illustration above, here is how the indexer works:_

* Kafka has a topic named, say `Topic1`

* Lets say, `Topic1` has 5 partitions.

* In the configuration file, kafka-es-indexer.properties, set firstPartition=0 and lastPartition=4 properties 

* start the indexer application as described below 

* there will be 5 threads started, one for each consumer from each of the partitions

* when a new partition is added to the kafka topic - configuration has to be updated and the indexer application has to be restarted


# How to use ? 

### Running as a standard Jar 

**1. Download the code into a `$INDEXER_HOME` dir.

**2. cp `$INDEXER_HOME`/src/main/resources/kafka-es-indexer.properties.template /your/absolute/path/kafka-es-indexer.properties file - update all relevant properties as explained in the comments

**3. cp `$INDEXER_HOME`/src/main/resources/logback.xml.template /your/absolute/path/logback.xml

 specify directory you want to store logs in:
	<property name="LOG_DIR" value="/tmp"/>
	
 adjust values of max sizes and number of log files as needed

**4. build/create the app jar (make sure you have MAven installed):

		cd $INDEXER_HOME
     	mvn clean package
     	
 The kafka-es-indexer-2.0.jar will be created in the $INDEXER_HOME/bin.
 All dependencies will be placed into $INDEXER_HOME/bin/lib.
 All JAR dependencies are linked via kafka-es-indexer-2.0.jar manifest.

**5. edit your $INDEXER_HOME/run_indexer.sh script:
		-- make it executable if needed (chmod a+x $INDEXER_HOME/run_indexer.sh)
		-- update properties marked with "CHANGE FOR YOUR ENV" comments - according to your environment

**6. run the app [use JDK1.8] :  

		./run_indexer.sh
 
# Versions

### Kafka Version: 0.8.2.1

### ElasticSearch: > 1.5.1

### Scala Version for Kafka Build: 2.10.0

# Configuration

Indexer app configuration is specified in the kafka_es_indexer.properties file, which should be created from a provided template, kafka-es-indexer.properties.template. All properties are described in the template:

[kafka-es-indexer.properties.template](https://github.com/ppine7/kafka-elasticsearch-standalone-consumer/blob/master/src/main/resources/kafka-es-indexer.properties.template)

Logging properties are specified in the logback.xml file, which should be created from a provided template, logback.xml.template: 

[logback.xml.template](https://github.com/ppine7/kafka-elasticsearch-standalone-consumer/blob/master/src/main/resources/logback.xml.template)


# Message Handler Class

*  `org.elasticsearch.kafka.consumer.MessageHandler` is an Abstract class that has most of the functionality of reading data from Kafka and batch-indexing into ElasticSearch already implemented. It has one abstract method, `transformMessage()`, that can be overwritten in the concrete sub-classes to customize message transformation before posting into ES

* `org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler` is a simple concrete sub-class of the MessageHAndler that sends messages into ES with no additional transformation, as is, in the 'UTF-8' format

* Usually, its effective to Index the message in JSON format in ElasticSearch. This can be done using a Mapper Class and transforming the message from Kafka by overriding/implementing the `transformMessage()` method. An example can be found here: `org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler`

* _**Do remember to set the newly created message handler class in the `messageHandlerClass` property in the kafka-es-indexer.properties file.**_

# IndexHandler Interface and basic implementation

*  `org.elasticsearch.kafka.consumer.IndexHandler` is an interface that defines two methods: getIndexName(params) and getIndexType(params). 

* `org.elasticsearch.kafka.consumer.BasicIndexHandler` is a simple imlementation of this interface that returnes indexName and indexType values as configured in the kafkaESConsumer.properties file. 

* one might want to create a custom implementation of IndexHandler if, for example, index name and type are not static for all incoming messages but depend on the event data - for example customerId, orderId, etc. In that case, pass all info that is required to perform that custom index determination logic as a Map of parameters into the getIndexName(params) and getIndexType(params) methods (or pass NULL if no such data is required)

* _**Do remember to set the index handler class in the `indexHandlerClass` property in the kafka-es-indexer.properties file. By default, BasicIndexHandler is used**_

# License

kafka-elasticsearch-standalone-consumer

	Licensed under the Apache License, Version 2.0 (the "License"); you may
	not use this file except in compliance with the License. You may obtain
	a copy of the License at

	     http://www.apache.org/licenses/LICENSE-2.0

	Unless required by applicable law or agreed to in writing,
	software distributed under the License is distributed on an
	"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
	KIND, either express or implied.  See the License for the
	specific language governing permissions and limitations
	under the License.

# Contributors
 - [Krishna Raj](https://github.com/reachkrishnaraj)
 - [Marina Popova](https://github.com/ppine7)
 - [Dhyan ](https://github.com/dhyan-yottaa)
