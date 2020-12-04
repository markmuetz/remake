from remake import TaskControl, remake_task_control, remake_required, task_declaration as task_dec


def join_lines(path, prepend_text):
    return '\n'.join([f'{prepend_text} {l}' for l in path.read_text().split('\n')[:-1]]) + '\n'


@remake_required(depends_on=[join_lines])
def f1(inputs, outputs):
    outputs[0].write_text(join_lines(inputs[0], 'f1'))


@remake_required(depends_on=[join_lines])
def f2(inputs, outputs):
    outputs[0].write_text(join_lines(inputs[0], 'f2'))


@remake_task_control
def gen_task_ctrl():
    """Task control which demonstrates @remake_required(depends_on=[...])

    Uses task_declaration (task_dec)

    in1.txt -> out1.txt -> out2.txt

    :return: task_ctrl
    """
    in1path = 'data/inputs/in1.txt'
    out1path = 'data/outputs/ex6/out1.txt'
    out2path = 'data/outputs/ex6/out2.txt'

    tasks_dec = [
        task_dec(f1, [in1path], [out1path]),
        task_dec(f2, [out1path], [out2path])
    ]
    return TaskControl.from_declaration(__file__, tasks_dec)
