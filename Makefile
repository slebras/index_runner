.PHONY: test


test:
	docker-compose down
	docker-compose run app sh scripts/run_tests.sh
	docker-compose down

reset:
	# Remove everything in the docker-compose cluster and rebuild
	docker-compose down -v --remove-orphans --rmi all
	docker-compose build --no-cache
