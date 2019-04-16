.PHONY: test

test:
	docker-compose restart web && docker-compose run web sh /app/scripts/run_tests.sh
