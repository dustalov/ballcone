export LANG := en_US.UTF-8

PIPENV := nice pipenv run

-include Makefile.local

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

test:
	$(PIPENV) ruff check .
	$(PIPENV) test

docker:
	docker build --rm -t ballcone .

pipenv:
	pipenv install --dev
