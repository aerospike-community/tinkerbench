# This must be run from the root directory of the project
DIR_PATH=$(pwd)
mvn clean install
java -Dconfig=$DIR_PATH/conf/simple.properties -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar BenchmarkLoad