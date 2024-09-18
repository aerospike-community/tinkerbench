# This must be run from the root directory of the project
DIR_PATH=$(pwd)
java -Dconfig=$DIR_PATH/conf/shortread.properties -jar ./target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar BenchmarkShortRead