# ami-6d1df904
# c1.xlarge

# javac -d classes src/db_compare/db/PingServer.java 
# java -server -cp classes db_compare.db.PingServer

# java -server -Xmx6g -cp fleetdb-standalone.jar fleetdb.server -f /tmp/fleetdb-bench.fdb

# redis-server

# mkdir /tmp/h2-bench
# java -server -Xmx2g -cp lib/h2-1.2.126.jar org.h2.tools.Server -tcp -baseDir /tmp/h2-bench -tcpAllowOthers

# memcached

# mkdir /tmp/mongodb-bench
# mongod --dbpath /tmp/mongodb-bench

# cat /proc/meminfo

# mysql start and ensure database?

HOST=ec2-174-129-111-143.compute-1.amazonaws.com
USER=root
KEY=/Users/mmcgrana/.ssh/scratch.pem
DIR=fleetdb

set -e

case $1 in
  "setup")
  # ssh -i $KEY $USER@$HOST "yum -y install git-core rlwrap && mkdir -p $DIR"
  # ssh -i $KEY $USER@$HOST "yum -y install git-core rlwrap && mkdir -p $DIR"
  ;;

  "sync")
  rsync -ra --delete --exclude '.git' --rsh 'ssh -i '$KEY ./ $USER@$HOST:$DIR/
  rsync -a --rsh 'ssh -i '$KEY bin/clj.sh $USER@$HOST:$DIR/clj
  ;;
  
  "ssh")
  ssh -i $KEY $USER@$HOST
  ;;
  
  "exec")
  ssh -i $KEY $USER@$HOST "cd $DIR && $2"
  ;;
  
  *)
  echo "Unrecognized option: $1"
  ;;
esac


