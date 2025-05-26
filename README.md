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
By default, logs are displayed at the INFO level. To adjust the logging level or disable logs, configure this in your application. For example:

```
import logging
# To change to DEBUG level
logging.basicConfig(level=logging.DEBUG)

# To disable logs
logging.getLogger("hyrex").setLevel(logging.CRITICAL)
```

To configure Hyrex logs only:

```
logger = logging.getLogger("hyrex")
# Change log level
logger.setLevel(logging.DEBUG)
# Or disable logs
logger.setLevel(logging.CRITICAL)
```

Handlers and formatting options from the `logging` module are also supported.

For worker processes, the logging level can be set using the `log-level` flag on the `hyrex run-worker` CLI command.
