lint:
	black --check src
	ruff check src
	mypy src

