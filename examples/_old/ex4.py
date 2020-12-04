from remake import TaskControl, remake_task_control, task_declaration as task_dec


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

    Uses task_declaration (task_dec)

    in1.txt -> out1.txt --> out2_0.txt --> out3.txt
                        |-> out2_1.txt |
                        |-> out2_2.txt |
                        |-> out2_3.txt |
                        \-> out2_4.txt /

    Uses different function for each stage.
    :return: task_ctrl
    """
    loop_over = range(5)
    tasks_dec = [
        task_dec(f1, ['data/inputs/in1.txt'], ['data/outputs/ex4/out1.txt']),
        task_dec(f2, ['data/outputs/ex4/out1.txt'], ['data/outputs/ex4/out2_{i}.txt'],
                 loop_over={'i': loop_over}),
        task_dec(f3, [f'data/outputs/ex4/out2_{i}.txt' for i in loop_over], ['data/outputs/ex4/out3.txt']),
    ]
    return TaskControl.from_declaration(__file__, tasks_dec)
