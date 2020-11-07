from remake import TaskControl, Task, remake_task_control


def f1(inputs, outputs):
    assert len(inputs) == len(outputs)
    for i, o in zip(inputs, outputs):
        o.write_text('\n'.join([f'f1 {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


@remake_task_control
def gen_task_ctrl():
    """Basic task control which takes in1.txt -> out1.txt -> out2.txt

    Uses same function for both steps.
    :return: task_ctrl
    """
    task_ctrl = TaskControl(__file__)

    task_ctrl.add(Task(f1, ['data/inputs/in1.txt'], ['data/outputs/ex1/out1.txt']))
    task_ctrl.add(Task(f1, ['data/outputs/ex1/out1.txt'], ['data/outputs/ex1/out2.txt']))

    return task_ctrl

