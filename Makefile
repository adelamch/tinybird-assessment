.PHONY: setup run execute test clean

setup:  ## Install dependencies locally (dev + production).
	pipenv install --dev

run:  ## Build and run with Docker. Usage: make run YEAR=2024 MONTH=1
	@test -n "$(YEAR)" || (echo "YEAR is required. Example: make run YEAR=2024 MONTH=1" && exit 1)
	@test -n "$(MONTH)" || (echo "MONTH is required. Example: make run YEAR=2024 MONTH=1" && exit 1)
	docker build -t tinybird-assessment .
	docker run --rm -v "$(PWD)/output:/app/output" tinybird-assessment --year $(YEAR) --month $(MONTH)

execute:  ## Run locally without Docker. Usage: make execute YEAR=2024 MONTH=1
	@test -n "$(YEAR)" || (echo "YEAR is required. Example: make execute YEAR=2024 MONTH=1" && exit 1)
	@test -n "$(MONTH)" || (echo "MONTH is required. Example: make execute YEAR=2024 MONTH=1" && exit 1)
	pipenv run python -m src.main --year $(YEAR) --month $(MONTH)

test:  ## Run tests.
	pipenv run pytest tests/ -v

clean:  ## Remove output and Python caches.
	rm -rf output/ __pycache__ src/__pycache__ tests/__pycache__ .pytest_cache
