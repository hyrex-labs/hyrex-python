# hyrex-sdk

Hyrex is a modern, open-source task orchestration framework.

## Installation

`pip install hyrex`

### Running on your own infra:

#### Step 1: Database initialization

- Set `HYREX_DATABASE_URL` to your Postgres database connection string
- Run `hyrex init-db`

#### Step 2: Decorate your tasks

- Instantiate a Hyrex object wherever your tasks are defined:

```
from hyrex import Hyrex

hy = Hyrex(app_id="my-hyrex-app")
```

- Decorate your task:

```
def NameContext(BaseModel):
    name: str


@hy.task
def say_name(context: NameContext):
    print(context.name)
```

- Send your task to the Hyrex queue. A worker will pick it up from there.

```
say_name.send(NameContext(name="Bob"))
```

#### Step 3: Run your worker(s)

- Make sure `HYREX_DATABASE_URL` is set.
- Update this command with the module path to your Hyrex instance:

```
hyrex run-worker my_app.tasks:hy
```

## Logging

Hyrex uses Python's `logging` module for logging info about task queueing, worker status, etc.
By default, no logs are displayed. To turn on logs, configure this in your application. For example:

```
import logging
logging.basicConfig(level=logging.INFO)
```

To configure Hyrex logs only:

```
logger = logging.getLogger("hyrex")
logger.setLevel(logging.INFO)
```

Handlers and formatting options from the `logging` module are also supported.

For worker processes, the logging level can be set using the `log-level` flag on the `hyrex run-worker` CLI command.
