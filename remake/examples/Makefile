# No Makefile dependencies -- just a concise way of bundling some commands.
# N.B. remakefile.py is not used for testing remake examples.

all: 
	echo "Not used"

reset: clean
	git checkout -- data/inputs
	git checkout -- ex?.py

clean: 
	rm -rf data/outputs
	rm -rf .remake
	rm data/fan_out* data/out.txt
