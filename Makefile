create-network:
	docker network create spark-streaming-network

run-appliance:
	mvn clean package -pl producer -am
	docker-compose -f docker-compose-appliance.yaml up --build --force-recreate

run-consumer:
	mvn clean package -pl state-consumer -am
	docker-compose -f docker-compose-consumer.yaml up --build --force-recreate

ssh-to-consumer:
	docker-compose -f docker-compose-consumer.yaml exec consumer /bin/bash