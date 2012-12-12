## Hash Migration
> a simple migration tool based on hash code of the relative path

----
edit by 3
----
### dev
	mvn clean; mvn eclipse:clean;
	mvn eclipse:eclipse -DdownloadJavaDocs=true -DdownloadSources=true;

### test (using dev profile)
	mvn test
	or run junit test in eclipse

### run (using pub profile)
	mvn package assembly:assembly -P pub;
	eg1: java -jar HashMigration-1.0-jar-with-dependencies.jar db_dump_file 50
	eg2: java -jar HashMigration-1.0-jar-with-dependencies.jar db_dump_file 50 -t
----
edit by 1
edit by 2
