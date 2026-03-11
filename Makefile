.PHONY: run clean test bench stop

PORT ?= 8000

run: clean  ## Start the server (kills any existing instance first)
	uv run fastapi dev main.py --port $(PORT)

stop:  ## Stop the server
	@lsof -ti:$(PORT) | xargs kill 2>/dev/null || true
	@echo "Stopped server on port $(PORT)"

clean:  ## Remove DuckDB files and stop running server
	@lsof -ti:$(PORT) | xargs kill 2>/dev/null || true
	@rm -f movies.duckdb movies.duckdb.wal
	@echo "Cleaned up"

test:  ## Run test suite
	@lsof -ti:$(PORT) | xargs kill 2>/dev/null || true
	@rm -f movies.duckdb movies.duckdb.wal
	uv run pytest tests/ -v

bench: clean  ## Run benchmarks (starts server automatically)
	uv run fastapi dev main.py --port $(PORT) > /dev/null 2>&1 & \
	sleep 3 && uv run python benchmark.py movies.csv; \
	lsof -ti:$(PORT) | xargs kill 2>/dev/null || true

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'
