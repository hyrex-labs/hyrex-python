from hyrex import HyrexApp

from .tasks import hy as registry
from .workflow import hy as workflow_registry

app = HyrexApp("PythonTestingApp")

app.add_registry(registry)
app.add_registry(workflow_registry)
