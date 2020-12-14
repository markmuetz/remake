# No Makefile dependencies -- just a concise way of bundling some commands.

all: run_examples

test_cli: run_ex1
	remake run ex1
	remake run --one ex1
	remake run --force ex1
	remake run --reasons ex1
	remake run --executor multiproc ex1
	remake run --display task_dag ex1
	remake run-tasks ex1 --tasks 516e69
	remake run-tasks ex1 --rule Basic1
	remake ls-tasks ex1
	remake ls-files ex1
	remake remakefile-info ex1
	remake rule-info ex1 Basic1
	remake task-info ex1 516e69
	remake file-info ex1 data/outputs/ex1/out1.txt
	remake version

run_ex1:
	remake run --reasons ex1.py

# OK, this is a really lame way to test remake. But it's better than nothing for now.
test_ex1: reset
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	touch data/inputs/in1.txt
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	echo newline >> data/inputs/in1.txt
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	touch data/outputs/ex1/out1.txt
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	cp ex1.1.py ex1.py
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	cp ex1.2.py ex1.py
	remake run --reasons ex1.py
	remake run --reasons ex1.py
	echo newline >> data/outputs/ex1/out1.txt
	remake run --reasons ex1.py || true
	remake run --reasons ex1.py || true
	echo "All tasks SUCCESSFUL"

run_examples:
	remake remakefile-info --format short ex1.py ex2.py ex3.py ex4.py
	remake run --reasons ex1.py
	remake run --reasons ex2.py
	remake run --reasons ex3.py
	remake run --reasons ex4.py
	remake remakefile-info --format short ex1.py ex2.py ex3.py ex4.py

remakefile_info:
	remake remakefile-info --format short ex1.py ex2.py ex3.py ex4.py

reset: clean
	git checkout -- data/inputs
	git checkout -- ex?.py

clean: 
	rm -rf data/outputs
	rm -rf .remake
