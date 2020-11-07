from remake import TaskControl, Task, remake_task_control


def f1(inputs, outputs):
    for i, o in zip(inputs, outputs):
        o.write_text('\n'.join([f'f1 {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


def f2(inputs, outputs):
    assert len(inputs) == len(outputs)

    for i, o in zip(inputs, outputs):
        o.write_text('\n'.join([f'f22 {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


def f3(inputs, outputs):
    assert len(outputs) == 1

    o = outputs[0]
    output_text = []
    for i in inputs:
        output_text.extend([f'f2 {l}' for l in i.read_text().split('\n')[:-1]])
    o.write_text('\n'.join(output_text) + '\n')


@remake_task_control
def gen_task_ctrl():
    """Task control which demonstrates fan out/in (AKA map reduce)

    in1.txt -> out1.txt --> out2_0.txt --> out3.txt
                        |-> out2_1.txt |
                        |-> out2_2.txt |
                        |-> out2_3.txt |
                        \-> out2_4.txt /

    Uses different function for each stage.
    :return: task_ctrl
    """
    task_ctrl = TaskControl(__file__)

    task_ctrl.add(Task(f1, ['data/inputs/in1.txt'], ['data/outputs/ex3/out1.txt']))
    for i in range(5):
        task_ctrl.add(Task(f2, ['data/outputs/ex3/out1.txt'], [f'data/outputs/ex3/out2_{i}.txt']))

    task_ctrl.add(Task(f3,
                       [f'data/outputs/ex3/out2_{i}.txt' for i in range(5)],
                       ['data/outputs/ex3/out3.txt']))

    return task_ctrl

