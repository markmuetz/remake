remake [![Build Status](https://travis-ci.com/markmuetz/remake.svg?branch=master)](https://travis-ci.com/markmuetz/remake) [![codecov](https://codecov.io/gh/markmuetz/remake/branch/master/graph/badge.svg)](https://codecov.io/gh/markmuetz/remake) 
======

Remake is a smart Python build tool, similar to `make`. It is file based -- all inputs and outputs of each task are files. It uses a pure-Python implementation to define a set of tasks, where any tasks can depend on the output from previous tasks. It makes it easy to define complex task graphs, using a filename formatter for each task to define its inputs and outputs. It is smart, in that if any of the tasks or any of the input files to a task's content changes, those tasks will be rerun. Subsequent tasks will only be rerun if their input has changed.

Remake tracks the contents of each file and task, and can be used to generate a report of how any particular file was made. It is particularly suited to use in scientific settings, due to its ability to reliably recreate any set of output files, based on running only those tasks that are necessary.

Simple demonstration
--------------------

```python
"""Simple remake file: in.txt -> fan_out1.txt -> out.txt
                              `> fan_out2.txt /
"""
from remake import Remake, TaskRule

demo = Remake()


class FanOut(TaskRule):
    rule_inputs = {'in': 'data/in.txt'}
    rule_outputs = {'fan_out_{i}': 'data/fan_out_{i}.txt'}
    var_matrix = {'i': [1, 2]}

    def rule_run(self):
        # self.inputs and self.outputs are dictionaries created from rule_inputs
        # Each value is a pathlib.Path.
        input_value = self.inputs['in'].read_text()
        self.outputs[f'fan_out_{self.i}'].write_text(f'FanOut {self.i}: {input_value}')


class Out(TaskRule):
    rule_inputs = {f'fan_out_{i}': f'data/fan_out_{i}.txt'
                   for i in [1, 2]}
    rule_outputs = {'out': 'data/out.txt'}

    def rule_run(self):
        input_values = []
        for i in [1, 2]:
            input_values.append(self.inputs[f'fan_out_{i}'].read_text())
        self.outputs['out'].write_text(''.join(input_values))

```

```bash
$ cat data/in.txt
input

$ remake run demo
=> demo <=
Build task DAG
Perform topological sort
Assign status to tasks
Status (complete/rescan/pending/remaining/cannot run): 0/1/0/3/0
Rescanning: /home/markmuetz/projects/remake/remake/examples/data/in.txt
1/3: 3491ba8fdd FanOut(i=1)
2/3: df3b4d6329 FanOut(i=2)
3/3: a20a6e7a29 Out()
Status (complete/rescan/pending/remaining/cannot run): 3/0/0/0/0

# edit FanOut.rule_run so that its output is different
$ remake run demo
=> demo <=
Build task DAG
Perform topological sort
Assign status to tasks
Status (complete/rescan/pending/remaining/cannot run): 0/0/2/1/0
1/3: 3491ba8fdd FanOut(i=1)
2/3: df3b4d6329 FanOut(i=2)
3/3: a20a6e7a29 Out()
Status (complete/rescan/pending/remaining/cannot run): 3/0/0/0/0

# edit Out.rule_run
$ remake run demo
=> demo <=
Build task DAG
Perform topological sort
Assign status to tasks
Status (complete/rescan/pending/remaining/cannot run): 2/0/1/0/0
3/3: a20a6e7a29 Out()
Status (complete/rescan/pending/remaining/cannot run): 3/0/0/0/0

$ cat data/out.txt 
FanOut 1: input
FanOut 2: input

```

Documentation is available at [http://markmuetz.github.io/remake/](http://markmuetz.github.io/remake/).

