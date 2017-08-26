simple client to fetch the complete index with the new Elasticsearch Java REST API and scroll endpoint

1) compile the jar:
mvn package assembly:assembly

2) to get some help 

java -jar target/esclient-1.0-SNAPSHOT-jar-with-dependencies.jar

3) to fetch the documents
a) java -jar target/esclient-1.0-SNAPSHOT-jar-with-dependencies.jar  --host=edoc-vmtest.ub.unibas.ch --port=80 --user=[ask martin for the user] --password='[ask Martin for the password]' > edoc_nested.all.json &  
or
b) java -jar target/esclient-1.0-SNAPSHOT-jar-with-dependencies.jar  --host=edoc-vmtest.ub.unibas.ch --port=80 --user=[ask martin for the user] --password='ask martin for the password' --endpoint=edoc_flattened  > edoc_flattened.all.json &

 
