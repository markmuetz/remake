from pathlib import Path

from remake import TaskControl, remake_task_control, task_declaration as task_dec


def f1(inputs, outputs):
    outputs['OUT1'].write_text('\n'.join([f'f1 {l}' for l in inputs['IN1'].read_text().split('\n')[:-1]]))


def f2(inputs, outputs, i, animal):
    for k, o in outputs.items():
        if k == (i, animal):
            o.write_text('\n'.join([f'my vars {i} {animal}: {line}'
                                    for line in inputs['IN2'].read_text().split('\n')[:-1]]) + '\n')
        else:
            o.write_text('\n'.join([f'not my vars: {line}'
                                    for line in inputs['IN2'].read_text().split('\n')[:-1]]) + '\n')


def f3(inputs, outputs, i, animal):
    assert len(inputs) == 1
    assert len(outputs) == 1
    output_text = []
    output_text.extend([f'f3 {i} {animal} {l}' for l in inputs[f'IN3_{i}_{animal}'].read_text().split('\n')[:-1]])
    k, o = next(iter(outputs.items()))
    o.write_text('\n'.join(output_text) + '\n')


def f4(inputs, outputs, i):
    assert len(outputs) == 1
    output_text = []
    for k, inputpath in inputs.items():
        output_text.extend([f'f4 {i} {l}' for l in inputpath.read_text().split('\n')[:-1]])

    k, o = next(iter(outputs.items()))
    o.write_text('\n'.join(output_text) + '\n')


def f5(inputs, outputs):
    assert len(outputs) == 1
    output_text = []
    for k, inputpath in inputs.items():
        output_text.extend([f'f5 {l}' for l in inputpath.read_text().split('\n')[:-1]])

    k, o = next(iter(outputs.items()))
    o.write_text('\n'.join(output_text) + '\n')


@remake_task_control
def gen_task_ctrl():
    """Task control which demonstrates a few more things:
    * inputs can be a dict as well as a list
    * filepaths can be a Path object
    * two loop vars can be used
    * loop vars can be passed as arg

    in1.txt -> out1.txt -1-M-> out2_{i}_{animal}.txt -1-1->
               out3_{i}_{animal}.txt -M-1-> out4_{i}.txt -M-1-> out5.txt

    i = range(5)
    animal = ["cat", "dog"]

    -1-M-> one to many
    -1-1-> one to one
    -1-1-> many to one

    Uses different function for each stage.
    :return: task_ctrl
    """
    loop1 = range(5)
    loop2 = ['cat', 'dog']

    indir = Path('data/inputs')
    outdir = Path('data/outputs/ex5')

    tasks_dec = [
        # Uses dict and Path for inputs/outputs.
        task_dec(f1, {'IN1': indir / 'in1.txt'}, {'OUT1': outdir / 'out1.txt'}),
        # 2 loop vars, passes loop vars.
        # Note substituted into keys and values of dict.
        task_dec(f2,
                 {'IN2': outdir / 'out1.txt'},
                 {'OUT2_{i}_{animal}': outdir / 'out2_{i}_{animal}.txt'},
                 loop_over={'i': loop1, 'animal': loop2}, pass_loop_vars=True),
        # 1 to 1 for each task.
        task_dec(f3,
                 {'IN3_{i}_{animal}': outdir / 'out2_{i}_{animal}.txt'},
                 {'OUT3_{i}_{animal}': outdir / 'out3_{i}_{animal}.txt'},
                 loop_over={'i': loop1, 'animal': loop2}, pass_loop_vars=True),
        # Close animal loop.
        # Note double brackets on {{i}}.
        task_dec(f4,
                 {f'IN4_{{i}}_{animal}': outdir / f'out3_{{i}}_{animal}.txt'
                  for animal in loop2},
                 {'OUT4_{i}': outdir / 'out4_{i}.txt'},
                 loop_over={'i': loop1}, pass_loop_vars=True),
        # Close i loop.
        task_dec(f5,
                 {f'OUT4_{i}': outdir / f'out4_{i}.txt'
                  for i in loop1},
                 {'OUT5': outdir / 'out5.txt'}),
    ]
    return TaskControl.from_declaration(__file__, tasks_dec)
