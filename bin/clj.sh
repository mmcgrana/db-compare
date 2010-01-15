#!/bin/bash 

CP=./:src/:classes/:scratch/
for file in lib/*.jar; do
  CP=$CP:$file
done

JAVA="java -server -Xmx7g -cp $CP"

if [ -z "$1" ]; then
  rlwrap $JAVA clojure.contrib.repl_ln
else
  $JAVA clojure.main $@
fi