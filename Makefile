export LANG := en_US.UTF-8

-include Makefile.local

run: ballcone/__main__.py | .venv-installed
	PYTHONPATH=$(CURDIR) nice venv/bin/python3 $<

pyinstaller: ballcone.spec | .venv-installed
	nice venv/bin/pyinstaller $<

test:
	python3 -munittest discover

mypy:
	mypy --ignore-missing-imports $(shell git ls-files '*.py')

docker:
	docker build --rm -t ballcone .

.venv-installed: requirements.txt requirements-dev.txt
	python3 -mvenv venv
	venv/bin/python3 -mpip install -U pip
	venv/bin/pip3 install -r requirements.txt
	venv/bin/pip3 install -r requirements-dev.txt
	venv/bin/pip3 --version
	venv/bin/pip3 list > $@
