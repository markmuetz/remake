from pathlib import Path

from remake import Remake, TaskRule

remake = Remake()


class AbsolutePathsTasks(TaskRule):
    # Doesn't matter if these don't exist.
    rule_inputs = {'in': Path('/tmp/data/in.txt')}
    rule_outputs = {'out': Path('/tmp/data/out.txt')}

    def rule_run(self):
        self.outputs['out'].write_text('done')
