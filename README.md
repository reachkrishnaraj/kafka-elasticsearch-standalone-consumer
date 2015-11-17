# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

## Please use "branch2.0" branch for enhanced version of this project


## Illustration of kafka-elasticsearch-standalone-consumer usage

### The consumer is positioned in the middle.

![](https://raw.githubusercontent.com/reachkrishnaraj/kafka-elasticsearch-standalone-consumer/master/img/Kafka_ES_Illustration_New.png)


# Introduction

### **Kafka Standalone Consumer will read the messages from Kafka, processes and index them in ElasticSearch.**

### **Easily Scaleable & Extendable !**

### _As described in the illustration above, here is how the StandAlone Consumer works:_

* Kafka has a topic named, say `Topic_1`

* Lets say, `Topic_1` has 5 partitions.

* Now, there is a needed to read, process the messages from Kafka and ElasticSearch

* In order to do that, have 5 Config Files and start 5 instances of this Standalone Consumer by tying each config file to the respective Consumer Instance.

* Now, we will have 5 Consumer Standalone Daemons running, listening & processing messages from each partition of `Topic_1` in Kafka.

* When there is a new partitions(say `6th partition`) in the same `Topic_1`, then start a new Consumer Daemon instance pointing to the new partition(`i.e: 6th partition`)

* This way, there is a clear way of subscribing and processing messages from multiple partitions across multiple topics using this Stand alone Consumer.

# How to use ? 

### Method 1: running as a standard Jar 

**1. Download the code into a `$CONSUMER_HOME` dir.

**2. cp `$CONSUMER_HOME`/src/main/resources/kafkaESConsumer.properties.template /your/absolute/path/kafkaESConsumer.properties file - update all relevant properties as explained in the comments

**3. cp `$CONSUMER_HOME`/src/main/resources/logback.xml.template /your/absolute/path/logback.xml

 specify directory you want to store logs in:
	<property name="LOG_DIR" value="/tmp"/>
	
 adjust values of max sizes and number of log files as needed

**4. build/create the app jar:

		cd $CONSUMER_HOME
     	mvn clean package
     	
	The kafka-es-consumer-0.2.jar will be created in the $CONSUMER_HOME/bin, with all dependencies included into the JAR

**5. run the app [use JDK1.8] :  

		java -Dlogback.configurationFile=/your/absolute/path/logback.xml -jar $CONSUMER_HOME/bin/kafka-es-consumer-0.2.jar /your/absolute/path/kafkaESConsumer.properties

 

### Method 2: running via JSVC as a Daemon

**1. Download the code. Let's say, `$CONSUMER_HOME` contains the code.**

**2. From the `$CONSUMER_HOME`, build the maven project.** - _this step will create the JAR file with all Consumer dependencies inside, in the ` $CONSUMER_HOME/bin ` directory_

    mvn clean package

**3. Create a config file for the Consumer Instance and provide all necessary properties.** - _Use the existing Config file `$CONSUMER_HOME`/src/main/resources/kafkaESConsumer.properties.template` as template._

    cp $CONSUMER_HOME/src/main/resources/kafkaESConsumer.properties $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

    vi $CONSUMER_HOME/src/main/resources/<consumerGroupName><topicName><PartitionNum>.properties - Edit & provide the correct config details.


_These files will be copied into the $CONSUMER_HOME/bin/classes/ dir after the build._
_The details & guide for each property in the config file is given in the property file itself._


**4. Start the Consumer as follows:**


    cd $CONSUMER_HOME/scripts
    
    vi consumerNew.sh
    
    Provide the value for all the below variables:
    
    # Setup variables
    #Set the full path of top directory of this kafka consumer
    base_dir=
    JAVA_HOME=
    #User as which the Consumer Daemon has to be run
    USER=

    ./consumerNew.sh -p start -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

    # ' -p ' - Can take either start | stop | restart
    
    # ' -c ' - the config file for the consumer instance with path 
    # (e.g: '$CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties')

**5. Confirm the successful start of the Consumer by looking into:**

_The below log file contains INFO during starting, restarting & stopping the Consumer Instance._

    #'$consumerGroupName,$topic,$partition' - properties as defined in the consumer instances's config file (i.e: '<consumerGroupName><topicName><PartitionNum>.properties' in this example
    
    vi $CONSUMER_HOME/processLogs/<$consumerGroupName>_<$topic>_<$partition>.out

_The below log file contains ERROR during starting, restarting & stopping the Consumer Instance._

    #'$consumerGroupName,$topic,$partition' - properties as defined in the consumer instances's config file (i.e: '<consumerGroupName><topicName><PartitionNum>.properties' in this example

    vi $CONSUMER_HOME/processLogs/<$consumerGroupName>_<$topic>_<$partition>.err

**6. Monitor the processing in the log file defined by the following property in the Consumer's Respective Config file.**


**7. To Stop the Consumer Instance:**

    cd $CONSUMER_HOME/scripts

    ./consumerNew.sh -p stop -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

**8. To Restart the Consumer Instance:**

    cd $CONSUMER_HOME/scripts

    ./consumerNew.sh -p restart -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

# Versions:

### Kafka Version: 0.8.2.1

### ElasticSearch: > 1.5.1

### Scala Version for Kafka Build: 2.10.0

# Configuring the Consumer Instance:

The details of each config property can be seen in the template file (below)

[Config File with details about each property](https://github.com/ppine7/kafka-elasticsearch-standalone-consumer/blob/master/src/main/resources/kafkaESConsumer.properties)

# Message Handler Class

*  `org.elasticsearch.kafka.consumer.MessageHandler` is an Abstract class that has most of the functionality of reading data from Kafka and batch-indexing into ElasticSearch already implemented. It has one abstract method, `transformMessage()`, that can be overwritten in the concrete sub-classes to customize message transformation before posting into ES

* `org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler` is a simple concrete sub-class of the MessageHAndler that sends messages into ES with no additional transformation, as is, in the 'UTF-8' format

* Usually, its effective to Index the message in JSON format in ElasticSearch. This can be done using a Mapper Class and transforming the message from Kafka by overriding/implementing the `transformMessage()` method. An example can be found here: `org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler`

* _**Do remember to set the newly created message handler class in the `messageHandlerClass` config property of the consumer instance.**_

# IndexHandler Interface and basic implementation

*  `org.elasticsearch.kafka.consumer.IndexHandler` is an interface that defines two methods: getIndexName(params) and getIndexType(params). 

* `org.elasticsearch.kafka.consumer.BasicIndexHandler` is a simple imlementation of this interface that returnes indexName and indexType values as configured in the kafkaESConsumer.properties file. 

* one might want to create a custom implementation of IndexHandler if, for example, index name and type are not static for all incoming messages but depend on the event data - for example customerId, orderId, etc. In that case, pass all info that is required to perform that custom index determination logic as a Map of parameters into the getIndexName(params) and getIndexType(params) methods (or pass NULL if no such data is required)

* _**Do remember to set the index handler class in the `indexHandlerClass` property in the kafkaESConsumer.properties file. By default, BasicIndexHandler is used**_

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
 - [Marina Popova](https://github.com/ppine7)
 - [Dhyan ](https://github.com/dhyan-yottaa)
 - [Krishna Raj](https://github.com/reachkrishnaraj)
