check:
	uv pip install -r requirements.txt
	mypy .
	ruff check .
	ruff format .
	pre-commit run --all-files