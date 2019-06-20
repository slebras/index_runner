.PHONY: test


test:
	docker-compose up -d
	sleep 15
	docker-compose exec app sh scripts/run_tests.sh
	docker-compose down -v --remove-orphans

reset:
	# Remove everything in the docker-compose cluster and rebuild
	docker-compose down -v --remove-orphans --rmi all
	docker-compose build --no-cache
