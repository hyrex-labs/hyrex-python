from hyrex import HyrexApp
from .tasks import hy as registry

app = HyrexApp("PythonTestingApp")

app.add_registry(registry)
