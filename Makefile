.PHONY: test


test:
	docker-compose up -d
	docker-compose exec app python -m test.wait_for_service
	docker-compose exec app sh scripts/run_tests.sh
	docker-compose down

reset:
	# Remove everything in the docker-compose cluster and rebuild
	docker-compose down -v --remove-orphans --rmi all
	docker-compose build --no-cache
