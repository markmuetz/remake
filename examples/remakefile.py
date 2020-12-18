"""Put remake through its paces by calling remake on all examples.

Best run with `remake -W run remakefile`, which disables info logging from this.
Dogfooding by using this instead of Makefile (previous), although it's a bit meta.
Allows easy broadcast on e.g. all examples remakefiles, and using different executors.
"""
import subprocess
from remake import TaskRule, Remake

remake_all = Remake()


def sysrun(command):
    """Streams output from command to stdout"""
    return subprocess.run(command, check=True, shell=True, encoding='utf8')


def run_commands(commands):
    for command in commands:
        print(command)
        output = sysrun(command)
        assert output.returncode == 0


VAR_MATRIX = {
    'name': ['ex1', 'ex2', 'ex3', 'ex4', 'ex5', 'ex6'],
    'executor': ['singleproc', 'multiproc']
}


# What to aim for:
# class RunAllRemakes(CommandTaskRule):
class RunAllRemakes(TaskRule):
    rule_inputs = {}
    rule_outputs = {'dummy': 'remakefile/run_all_remakes.{name}.{executor}.run'}
    var_matrix = VAR_MATRIX
    force = True

    # What to aim for:
    # command = 'remake run {name}'
    def rule_run(self):
        executor = f'-E {self.executor}'
        commands = [
            f'rm -rf data/outputs/{self.name}',
            f'remake run {executor} {self.name}',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


class TestCLI(TaskRule):
    rule_inputs = {}
    rule_outputs = {'dummy': 'remakefile/test_cli.{name}.run'}
    var_matrix = {'name': VAR_MATRIX['name']}
    force = True

    def rule_run(self):
        commands = [
            f'remake run {self.name}',
            f'remake run --one {self.name}',
            f'remake run --force {self.name}',
            f'remake run --reasons {self.name}',
            f'remake run --executor multiproc {self.name}',
            f'remake run --display task_dag {self.name}',
            # ex1 specific.
            # f'remake run-tasks {self.name} --tasks 516e69',
            # f'remake run-tasks {self.name} --rule Basic1',
            f'remake ls-tasks {self.name}',
            f'remake ls-files {self.name}',
            f'remake info {self.name}',
            # ex1 specific.
            # f'remake rule-info {self.name} Basic1',
            # ex1 specific.
            # f'remake task-info {self.name} 516e69',
            f'remake file-info {self.name} data/outputs/{self.name}/out1.txt',
            f'remake version',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


class TestEx1(TaskRule):
    inputs = {}
    outputs = {'dummy': 'remakefile/test_ex1.run'}
    force = True

    def rule_run(self):
        commands = [
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'touch data/inputs/in1.txt',
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'echo newline >> data/inputs/in1.txt',
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'touch data/outputs/ex1/out1.txt',
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'cp ex1.1.py ex1.py',
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'cp ex1.2.py ex1.py',
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'echo newline >> data/outputs/ex1/out1.txt',
            'remake run --reasons ex1.py || true',
            'remake run --reasons ex1.py || true',
            'echo "All tasks SUCCESSFUL"',
            'make reset',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


if __name__ == '__main__':
    remake_all.finalize()
