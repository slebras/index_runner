.PHONY: test

test:
	docker-compose restart web && docker-compose run web sh /app/scripts/run_tests.sh

reset:
	docker-compose down -v  --remove-orphans --rmi all
	docker-compose build --no-cache
	docker-compose up
