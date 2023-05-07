.PHONY: install
install:
	pip install \
		pytest \
		pytest-postgres \
		-e ./postgres-copy-binary-python/ \
		./postgres-copy-binary-extension-module/

.PHONY: test
test:
	pytest tests/
