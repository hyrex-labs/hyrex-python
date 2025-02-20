import threading

_current_builder = threading.local()


def get_current_workflow_builder():
    return getattr(_current_builder, "builder", None)


def set_current_workflow_builder(builder):
    _current_builder.builder = builder


def clear_current_workflow_builder():
    _current_builder.builder = None
