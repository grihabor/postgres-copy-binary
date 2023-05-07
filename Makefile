.PHONY: install
install:
	pip install -e ./postgres-copy-binary-python/ ./postgres-copy-binary-extension-module/

.PHONY: test
test:
	pytest tests/
