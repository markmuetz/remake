from remake import TaskControl, remake_task_control, task_declaration as task_dec


def f1(inputs, outputs):

    print(inputs)

    for i, o in zip(inputs, outputs):
        o.write_text('\n'.join([f'f1 {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


def f2(inputs, outputs):
    print(inputs)

    assert len(inputs) == len(outputs)

    for i, o in zip(inputs, outputs):
        o.write_text('\n'.join([f'f22 {l}' for l in i.read_text().split('\n')[:-1]]) + '\n')


def f3(inputs, outputs):
    print(inputs)
    assert len(outputs) == 1

    o = outputs[0]
    output_text = []
    for i in inputs:
        output_text.extend([f'f2 {l}' for l in i.read_text().split('\n')[:-1]])
    o.write_text('\n'.join(output_text) + '\n')


@remake_task_control
def gen_task_ctrl():
    loop_over = range(5)
    tasks_dec = [
        task_dec(f1, ['data/inputs/in1.txt'], ['data/outputs/ex4/out1.txt']),
        task_dec(f2, ['data/outputs/ex4/out1.txt'], ['data/outputs/ex4/out2_{i}.txt'],
                 loop_over={'i': loop_over}),
        task_dec(f3, [f'data/outputs/ex4/out2_{i}.txt' for i in loop_over], ['data/outputs/ex4/out3.txt']),
    ]
    return TaskControl.from_declaration(__file__, tasks_dec)
