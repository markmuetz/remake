"""Put remake through its paces by calling remake on all examples.

Best run with `remake -W run remakefile`, which disables info logging from this.
Dogfooding by using this instead of Makefile (previous), although it's a bit meta.
Allows easy broadcast on e.g. all examples remakefiles, and using different executors.
"""
from pathlib import Path
import subprocess
from remake import TaskRule, Remake, remake_cmd

remake_all = Remake()


def sysrun(command):
    """Streams output from command to stdout"""
    return subprocess.run(command, check=True, shell=True, encoding='utf8')


def run_commands(commands):
    for command in commands:
        print(command)
        system = command.split()[0] != 'remake'
        if system:
            output = sysrun(command)
            assert output.returncode == 0
        else:
            remake_cmd.remake_cmd(['examples/remakefile.py'] + command.split()[1:])


VAR_MATRIX = {
    'name': sorted([p.name for p in Path.cwd().glob('ex?.py')]),
    'executor': ['singleproc', 'multiproc']
}


# What to aim for:
# class RunAllRemakes(CommandTaskRule):
class RunAllRemakes(TaskRule):
    rule_inputs = {}
    rule_outputs = {'dummy': 'remakefile_output/run_all_remakes.{name}.{executor}.run'}
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
    rule_outputs = {'dummy': 'remakefile_output/test_cli.{name}.run'}
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
            f'remake ls-tasks {self.name}',
            f'remake ls-tasks --long {self.name}',
            f'remake ls-files {self.name}',
            f'remake info {self.name}',
            f'remake file-info {self.name} data/outputs/{self.name}/out1.txt',
            f'remake version',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


class TestCLI2(TaskRule):
    rule_inputs = {}
    rule_outputs = {'dummy': 'remakefile_output/test_cli2.run'}
    force = True

    def rule_run(self):
        commands = [
            f'rm -rf data/outputs/ex2',
            f'remake info ex2',
            f'remake ls-tasks ex2',
            f'remake ls-tasks -l ex2',
            f'remake ls-tasks --filter=i=2 ex2',
            f'remake ls-tasks --uses-file=data/outputs/ex2/fan_out.0.3.out ex2',
            f'remake ls-tasks --produces-file=data/outputs/ex2/fan_out.0.3.out ex2',
            f'remake ls-files ex2',
            f'remake ls-files -l --input ex2',
            f'remake ls-files --output ex2',
            f'remake ls-files --input-only ex2',
            f'remake ls-files --output-only ex2',
            f'remake ls-files --inout ex2',
            f'remake ls-files --produced-by-rule=Process ex2',
            f'remake ls-files --used-by-rule=Process ex2',
            f'remake ls-files --exists ex2',
            f'remake ls-rules ex2',
            f'remake run-tasks --handle-dependencies --produces-file=data/outputs/ex2/fan_out.0.3.out ex2',
            f'remake run-tasks --rule=Init ex2',
            f'remake run-tasks --rule=FanOut ex2',
            f'remake run-tasks --rule=Process ex2',
            f'remake run --random ex2',
            f'remake run-tasks --rule=Reduce1 ex2',
            f'remake run-tasks --rule=Reduce2 ex2',
            f'remake rm-files -f --output-only ex2',
            f'remake rule-info ex2 Process',
            f'remake task-info ex2 82b155',
            f'remake file-info ex2 data/outputs/ex2/fan_out.0.3.out',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


class TestEx1(TaskRule):
    rule_inputs = {}
    rule_outputs = {'dummy': 'remakefile_output/test_ex1.run'}
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
            'remake run --reasons ex1.py',
            'remake run --reasons ex1.py',
            'echo "All tasks SUCCESSFUL"',
            'make reset',
        ]
        run_commands(commands)
        self.outputs['dummy'].touch()


if __name__ == '__main__':
    remake_all.finalize()
