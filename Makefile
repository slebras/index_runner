.PHONY: test

test:
	docker-compose restart web && docker-compose run web python -m unittest discover test
