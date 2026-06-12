from .executor import Executor
from .singleproc_executor import SingleprocExecutor
from .slurm_executor import SlurmExecutor

# The multiproc executor is pending adaptation to the remake3 API
# (see design_docs/detailed_code_implementation.md) and is not importable
# until then.
