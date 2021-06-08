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
	rm -f data/fan_out* data/out.txt
	rm -rf test_all_examples_output
	rm -rf __pycache__
	rm -rf invalid_remakefiles/__pycache__
	rm -rf invalid_remakefiles/__pycache__ invalid_remakefiles/.remake
