.PHONY: test


test:
	docker-compose up -d workspace
	docker-compose up -d zookeeper
	sleep 10
	docker-compose up -d kafka
	docker-compose up -d elasticsearch
	sleep 30
	docker-compose up -d app
	docker-compose exec app sh scripts/run_tests.sh
	docker-compose down -v --remove-orphans

reset:
	# Remove everything in the docker-compose cluster and rebuild
	docker-compose down -v --remove-orphans --rmi all
	docker-compose build --no-cache
