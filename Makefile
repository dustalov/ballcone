export LANG := en_US.UTF-8

MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules

DOCKER := $(if $(shell which podman),podman,docker)

PIPENV := nice pipenv run

test:
	$(PIPENV) mypy ballcone tools
	$(PIPENV) ruff check .
	$(PIPENV) python3 -m unittest discover

run: ballcone/__main__.py
	$(PIPENV) "$<"

pyinstaller: ballcone.spec
	$(PIPENV) pyinstaller "$<"

install-systemd:
	cp -Rvf dist/ballcone /usr/local/bin/ballcone
	mkdir -pv /var/lib/ballcone
	chown -Rv nobody:nobody /var/lib/ballcone
	cp -Rvf ballcone.service /etc/systemd/system/
	systemctl daemon-reload
	systemctl enable ballcone
	systemctl restart ballcone

docker:
	$(DOCKER) build -f Dockerfile --rm -t ballcone .

pipenv:
	pipenv install --dev

-include Makefile.local
