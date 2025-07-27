UV=uv
UV_RUN=$(UV) run
FIX=1
COVERAGE=0
VERSION=$(shell $(UV_RUN) dunamai from git)

default: help

.PHONY: sync
sync: ## Sync the venv
	$(UV) sync

.PHONY: lint
lint: ## Lint the code
ifeq ($(FIX), 1)
	$(UV_RUN) ruff check --fix .
	$(UV_RUN) ruff format .
else
	$(UV_RUN) ruff check .
	$(UV_RUN) ruff format --check .
endif
	$(UV_RUN) mypy --check-untyped-defs .

.PHONY: version
version: ## Print the version
	@echo $(VERSION)

.PHONY: test
test: ## Test the code
ifeq ($(COVERAGE), 1)
	$(UV_RUN) pytest --cov=async_redis_rate_limiters --cov-report=html --cov-report=term .
else
	$(UV_RUN) pytest .
endif

.PHONY: doc
doc: ## Generate the documentation
	$(UV_RUN) jinja-tree .

.PHONY: clean
clean: ## Clean the repository
	rm -Rf .venv .*_cache build dist htmlcov .coverage
	find . -type d -name __pycache__ -exec rm -Rf {} \; 2>/dev/null || true

.PHONY: no-dirty
no-dirty: ## Check that the repository is clean
	if test -n "$$(git status --porcelain)"; then \
		echo "***** git status *****"; \
		git status; \
		echo "***** git diff *****"; \
		git diff; \
		echo "ERROR: the repository is dirty"; \
		exit 1; \
	fi

.PHONY: set-version
set-version:
	$(UV_RUN) python set-version.py $(VERSION) pyproject.toml async_redis_rate_limiters/__init__.py

.PHONY: build
build: set-version ## Build the package
	$(UV) build

.PHONY: publish
publish: set-version ## Publish the package to PyPI
ifeq ($(UV_PUBLISH_TOKEN),)
	@echo "ERROR: UV_PUBLISH_TOKEN is not set" && exit 1
endif
	$(UV) publish

.PHONY: help
help:
	@# See https://marmelab.com/blog/2016/02/29/auto-documented-makefile.html
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
