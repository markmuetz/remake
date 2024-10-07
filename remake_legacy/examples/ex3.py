import calendar

from remake import Remake, TaskRule


ex3 = Remake()

YEARS_MONTHS = {
    'year': range(2015, 2021),
    'month': range(1, 13),
}


class VariableNumberOutput(TaskRule):
    rule_inputs = {}

    @staticmethod
    def rule_outputs(year, month):
        days_in_month = calendar.monthrange(year, month)[1]
        return {f'day{d}': f'data/outputs/ex3/{year}.{month:02}.{d:02}'
                for d in range(1, days_in_month + 1)}

    var_matrix = YEARS_MONTHS

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.year}, {self.month})'
        for key, o in self.outputs.items():
            o.write_text(f'{key} {payload}')


class VariableNumberOutput2(TaskRule):
    rule_inputs = VariableNumberOutput.rule_outputs
    rule_outputs = {'out': 'data/outputs/ex3/{year}.{month:02}.out'}
    var_matrix = YEARS_MONTHS

    def rule_run(self):
        payload = f'{self.__class__.__name__} ({self.year}, {self.month})'
        for o in self.outputs.values():
            o.write_text(payload)
