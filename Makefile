.PHONY: test

test:
	docker-compose restart web && docker-compose run web python -m test.test_daemon
