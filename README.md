# Spark Structured Streaming State Store Example

Example project with Spark Structured Streaming and StateStore. 

## How to run
- Create the docker network:
```shell script
make create-network
```
- Start the random data streaming generator and kafka appliance:
```shell script
make run-appliance
```
- Compile and run the consumer:
```shell script
make run-consumer
```
