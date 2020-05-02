export LANG := en_US.UTF-8

-include Makefile.local

run: .venv-installed
	nice venv/bin/ballcone

test:
	python3 -munittest discover

mypy:
	mypy --ignore-missing-imports $(shell git ls-files '*.py')

docker:
	docker build --rm -t ballcone .

.venv-installed: requirements.txt
	python3 -mvenv venv
	venv/bin/pip3 install -r $<
	venv/bin/pip3 --version
	venv/bin/python3 setup.py install > $@
