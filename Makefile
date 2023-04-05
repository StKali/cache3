lint:
	pylint src --rcfile=pylint.conf

build:
	$(shell if [ ! -e dist ]; then mkdir -p dist; else rm -rf dist/*; fi)
