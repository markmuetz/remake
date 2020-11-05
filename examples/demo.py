import sys
from time import sleep
from importlib import reload

from remake import task
from remake import task_control
from remake import multiproc_task_control

reload(task)
reload(task_control)
reload(multiproc_task_control)

def fn(inputs, outputs):
    total = 0
    for i in range(int(1e8)):
        total += i
    print(total)
    print(f'    {inputs}')
    print(f'    {outputs}')
    for o in outputs:
        o.touch()

if __name__ == '__main__':
    if sys.argv[1] == 'mp':
        TaskControl = multiproc_task_control.MultiProcTaskControl
        tc_kwargs = {'nproc': 4}
    else:
        TaskControl = task_control.TaskControl
        tc_kwargs = {}
    tc_kwargs['filename'] = __file__
    tcs = []

    tc1 = TaskControl(**tc_kwargs)

    tc1.add(task.Task(fn, ['in1'], ['out0']))
    tc1.add(task.Task(fn, ['out0'], ['out1']))
    tc1.add(task.Task(fn, ['out0'], ['out2']))
    tc1.add(task.Task(fn, ['out1'], ['out3']))
    tc1.add(task.Task(fn, ['out2'], ['out4']))
    tc1.add(task.Task(fn, ['out3', 'out4'], ['out5']))
    tc1.finalize()
    tcs.append(tc1)

    tc2 = TaskControl(**tc_kwargs)
    for i in range(8):
        for j in range(i, 8):
            if i == 0 and j == 0:
                tc2.add(task.Task(fn, ['in1'], [f'out_{i}_{j}']))
            elif i == 0:
                tc2.add(task.Task(fn, [f'out_{i}_{j - 1}'], [f'out_{i}_{j}']))
            elif i == j:
                tc2.add(task.Task(fn, [f'out_{i - 1}_{j - 1}'], [f'out_{i}_{j}']))
            else:
                tc2.add(task.Task(fn, [f'out_{i - 1}_{j - 1}', f'out_{i}_{j - 1}'], [f'out_{i}_{j}']))
    tc2.finalize()
    tcs.append(tc2)

    tc3 = TaskControl(**tc_kwargs)

    tc3.add(task.Task(fn, ['mp_in1'], ['mp_out0']))
    tc3.add(task.Task(fn, ['mp_out0'], ['mp_out1']))
    tc3.add(task.Task(fn, ['mp_out0'], ['mp_out2']))
    tc3.add(task.Task(fn, ['mp_out1'], ['mp_out3']))
    tc3.add(task.Task(fn, ['mp_out2'], ['mp_out4']))
    tc3.add(task.Task(fn, ['mp_out3', 'mp_out4'], ['mp_out5']))
    tc3.finalize()

    tcs.append(tc3)

    for tc in tcs:
        print(f'completed: {tc.completed_tasks}')
        print(f'pending  : {tc.pending_tasks}')
        print(f'remaining: {tc.remaining_tasks}')
        print(f'all      : {tc.tasks}')
        print()

