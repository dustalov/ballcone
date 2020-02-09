export LANG := en_US.UTF-8

-include Makefile.local

run: balcone.py | .venv-installed
	nice venv/bin/python3 $<

.venv-installed: requirements.txt
	python3 -mvenv venv
	venv/bin/pip3 install -r $<
	venv/bin/pip3 --version > $@
