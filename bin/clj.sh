#!/bin/bash 

CP=./:src/:classes/
for file in lib/*.jar; do
  CP=$CP:$file
done

JAVA="java -server -Xmx7g -cp $CP"

if [ -z "$1" ]; then
  rlwrap $JAVA clojure.contrib.repl_ln
else
 java $JAVA clojure.main $@
fi