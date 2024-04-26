# This must be run from the root directory of the project
DIR_PATH=$(pwd)
mvn clean install
java -Dconfig=$DIR_PATH/conf/stitch.properties -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar BenchmarkStitching