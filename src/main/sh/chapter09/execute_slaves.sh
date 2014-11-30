#!/bin/sh
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

HOSTLIST=slaves
if [ ! -n "$1" ]
then
echo "usage: $0 \"<command arg1>;<command arg2>;....;<command argN>\""
echo "Examples:"
echo "  $0 \"cd /home/hadoop; tar xvf hadoop.tar.gz\""
exit 1
fi

for i in `cat $HOSTLIST`
do
echo "exec" $i "$*"
  echo "Executing shell commands on $i"
  ssh $i "$*"
done