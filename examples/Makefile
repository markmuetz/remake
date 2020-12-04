# No Makefile dependencies -- just a concise way of bundling some commands.

run_examples:
	remake run --reasons ex1.py ex2.py ex3.py ex4.py ex5.py ex6.py ex_task_rule.py ex_task_rule2.py ex_task_rule3.py ex_task_rule4.py

task_control_info:
	remake task-control-info --format short ex1.py ex2.py ex3.py ex4.py ex5.py ex6.py ex_task_rule.py ex_task_rule2.py ex_task_rule3.py ex_task_rule4.py

clean: 
	rm -rf data/outputs
	rm -rf .remake
