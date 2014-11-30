#!/bin/sh
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

HOSTLIST=slaves
if [ 2 -gt "$#" ]
then
  echo "Usage: $0 \"<local file path> <remote file path>\""
  echo "Examples:"
  echo "  $0 \"/home/hadoop/hadoop.tar.gz /home/hadoop\""
  exit 1
fi

for i in `cat $HOSTLIST`
do
  echo "Copying $1 to $i:$2"
  scp "$1" $i:"$2"
done