create-network:
	docker network create spark-streaming-network

run-appliance:
	mvn clean package -pl producer
	docker-compose -f docker-compose-appliance.yaml up --build

run-consumer:
	mvn clean package -pl state-consumer
	docker-compose -f docker-compose-consumer.yaml up --build