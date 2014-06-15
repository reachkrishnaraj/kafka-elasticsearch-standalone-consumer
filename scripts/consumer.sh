#!/bin/sh

while getopts p:c: option
do
        case "${option}"
        in
                p) OPERATION=${OPTARG};;
                c) CONFIG_FILE=${OPTARG};;
        esac
done

### Trim leading whitespaces ###
OPERATION="${OPERATION##*( )}"
CONFIG_FILE="${CONFIG_FILE##*( )}"

#Get the Consumer Group Name for the Consumer Instance
CONSUMER_GROUP_NAME=`grep 'consumerGroupName=' $CONFIG_FILE | head -1 | cut -d "=" -f2 | tr -d ' '`

#Get the topic for the Consumer Instance
KAFKA_TOPIC=`grep 'topic=' $CONFIG_FILE | head -1 | cut -d "=" -f2 | tr -d ' '`

#Get the topic partition for the Consumer Instance
TOPIC_PARTITION=`grep 'partition=' $CONFIG_FILE | head -1 | cut -d "=" -f2 | tr -d ' '`

# Setup variables
#Get the relative directory from which this script is run
base_dir_relative=$(dirname $0)/..

#Get the absolute directory from which this scriprt is run
base_dir=$(cd $(dirname $base_dir_relative); pwd)/$(basename $base_dir_relative)


# create logs directory if it doesnt exist
LOG_DIR=$base_dir/logs
if [ ! -d $LOG_DIR ]; then
        mkdir $LOG_DIR
fi

# create logs directory if it doesnt exist
PROCESS_LOG_DIR=$base_dir/processLogs
if [ ! -d $PROCESS_DIR ]; then
        mkdir $PROCESS_DIR
fi

#Get the path where jsvc binary is available
EXEC=$base_dir/scripts/jsvc

#Get all the dependent library jars into classpath
for file in $base_dir/lib/dep-jars/*.jar;
do
  libClassPath=$libClassPath:$file
done

#Get the standalone consumer jar in the bin directory into the classpath
for file in $base_dir/bin/*.jar;
do
  libClassPath=$libClassPath:$file
done

#get the config dir and the base_dir into the classpath. This is needed to make the config file searchable
CLASS_PATH=$libClassPath:$base_dir"/config":$base_dir


#This is the main class from which the Consumer Daemon is started. PLEASE DONT CHANGE THIS !!
CLASS=org.elasticsearch.kafka.consumer.daemon.KafkaConsumerDaemon

#User as which the Consumer Daemon has to be run
USER=kraj

#This file stores the Process ID of the Consumer Daemon
PID=$PROCESS_LOG_DIR/$CONSUMER_GROUP_NAME"_"$KAFKA_TOPIC"_"$TOPIC_PARTITION".pid"

#This file contains the info when starting|stopping|restarting the consumer daemon
LOG_OUT=$PROCESS_LOG_DIR/$CONSUMER_GROUP_NAME"_"$KAFKA_TOPIC"_"$TOPIC_PARTITION".out"

#This file contains the errors when starting|stopping|restarting the consumer daemon
LOG_ERR=$PROCESS_LOG_DIR/$CONSUMER_GROUP_NAME"_"$KAFKA_TOPIC"_"$TOPIC_PARTITION".err"

echo $PID
echo $LOG_OUT
echo $LOG_ERR

case "$OPERATION" in
    start)
        do_start $CONFIG_FILE
            ;;
    stop)
        do_stop
            ;;
    restart)
        if [ -f "$PID" ]; then
            do_stop
            do_start $CONFIG_FILE
        else
            echo "Consumer Daemon not running, will do nothing."
            exit 1
        fi
            ;;
    *)
            echo "Usage: consumer.sh -p {start|stop|restart} -c <path_for_configFile_for_the_consumer_instance>" >&2
            exit 3
            ;;
esac

do_start()
{
      #echo $EXEC -home "$JAVA_HOME" -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID $CLASS $1 $2
      echo "Starting the Consume Daemon. Please wait......"
      $EXEC -home "$JAVA_HOME" -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID $CLASS $1
      echo "*** Start attempt completed.,"
      echo "*** Please check "$LOG_OUT" file for start confirmation and "
      echo $LOG_ERR" for errors in case of failure ***"
}


do_stop()
{
      #echo $EXEC -home "$JAVA_HOME" -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID -stop $CLASS
      echo "Stopping the Consumer Daemon. Please wait......"
      $EXEC -home "$JAVA_HOME" -cp $CLASS_PATH -user $USER -outfile $LOG_OUT -errfile $LOG_ERR -pidfile $PID -verbose -stop $CLASS
      echo "*** Stop attempt completed.,"
      echo "*** Please check "$LOG_OUT" file for stop confirmation and "
      echo $LOG_ERR" for errors in case of failure ***"
}