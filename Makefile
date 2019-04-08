.PHONY: test

test:
	docker-compose restart web && docker-compose run web sh /scripts/run_tests.sh
