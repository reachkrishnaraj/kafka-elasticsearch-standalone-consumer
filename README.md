# Welcome to the kafka-elasticsearch-standalone-consumer wiki!

## Illustration of kafka-elasticsearch-standalone-consumer usage

### The consumer is sitting in the middle.

![](https://raw.githubusercontent.com/reachkrishnaraj/kafka-elasticsearch-standalone-consumer/master/img/Kafka_ES_Illustration.png)


# Introduction

### **Kafka Standalone Consumer will read the messages from Kafka, processes and index them in ElasticSearch.**

### _As described in the illustration above, here is how the StandAlone Consumer works:_

* Lets say, Kafka has a topic named, say `Topic_1`

* Lets say, `Topic_1` has 5 partitions.

* Now, there is a needed to read, process the messages from Kafka and index it in ElasticSearch

* In order to do that, have 5 Config Files and start 5 instances of this Standalone Consumer by tying each config file to the respective Consumer Instance.

* Now, we will have 5 Consumer Standalone Daemons running, listening & processing messages from each partition of `Topic_1` in Kafka.

* When there is a new partitions(say `6th partition`) in the same `Topic_1`, then start a new Consumer Daemon instance pointing to the new partition(`i.e: 6th partition`)

* This way, there is a clear way of subscribing and processing messages from multiple partitions across multiple topics using this Stand alone Consumer.

# How to use ?

**1. Download the code. Let's say, `$CONSUMER_HOME` contains the code.**

**2. From the `$CONSUMER_HOME`, build the maven project.** - _this step will create the JAR file of the Consumer and the dependency files in the ` $CONSUMER_HOME/bin ` directory_

    mvn clean package

**3. Create a config file for the Consumer Instance and provide all necessary properties.** - _Use the existing Config file `$CONSUMER_HOME/config/kafkaESConsumer.properties` as template._

    cp $CONSUMER_HOME/config/kafkaESConsumer.properties $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

    vi $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties - Edit & provide the correct config details.


_The details & guide for each property in the config file is given in the property file itself._

_It is **IMPORTANT to SPECIFY 1 UNIQUE LOG PROPERTY FILE(using the below property) FOR EACH CONSUMER INSTANCE** in the respective config file to have logging happen in separate log file for each consumer instance._


    #Log property file for the consumer instance. One instance of consumer should have 1 log file.
    logPropertyFile=log4j<consumerGroupName><topicName><PartitionNum>.properties

**4. Start the Consumer as follows:**


    cd $CONSUMER_HOME/scripts

    ./consumer.sh -p start -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

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

(i.e: in this example, `$CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties`)

    Property/Config name: logPropertyFile

**7. To Stop the Consumer Instance:**

    cd $CONSUMER_HOME/scripts

    ./consumer.sh -p stop -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

**8. To Restart the Consumer Instance:**

    cd $CONSUMER_HOME/scripts

    ./consumer.sh -p restart -c $CONSUMER_HOME/config/<consumerGroupName><topicName><PartitionNum>.properties

# Versions:

### Kafka Version: 0.8.1

### ElasticSearch: 1.0.0

# Configuring the Consumer Instance:

The details of each config property can be seen in the template file (below)

    

# Message Handler Class

* If the message in your Kafka has to handled in Raw `UTF-8` text, then you can use message handler class `org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler`

* You can code your own Message Handler class by extending the abstract class `org.elasticsearch.kafka.consumer.MessageHandler` and implementing the methods: `transformMessage()` & `prepareForPostToElasticSearch()`.

* Also, if the message in your Kafka has to handled in Raw `UTF-8` text and you just want to change the way the raw message is transformed(into your desired format), then extend the `org.elasticsearch.kafka.consumer.messageHandlers.RawMessageStringHandler` class and override/implement the `transformMessage()` method alone. An example can be found here: `org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler`

* Usually, its effective to Index the message in JSON format in ElasticSearch. This can be done using a Mapper Class and transforming the message from Kafka by overriding/implementing the `transformMessage()` method. An example can be found here: `org.elasticsearch.kafka.consumer.messageHandlers.AccessLogMessageHandler`

* _**Do remember to set the newly created(if) message handler class in the `messageHandlerClass` config property of the consumer instance.**_

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
