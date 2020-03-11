.PHONY: test


test:
	docker-compose run app sh scripts/run_tests.sh

reset:
	# Remove everything in the docker-compose cluster and rebuild
	docker-compose down -v --remove-orphans --rmi all
	docker-compose build --no-cache
