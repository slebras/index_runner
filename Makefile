.PHONY: test

test:
	docker-compose restart app && docker-compose run app sh /app/scripts/run_tests.sh

reset:
	docker-compose down -v  --remove-orphans --rmi all
	docker-compose build --no-cache
	docker-compose up
