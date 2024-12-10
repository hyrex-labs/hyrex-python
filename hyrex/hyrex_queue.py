from pydantic import BaseModel


class HyrexQueue(BaseModel):
    name: str
    concurrency: int | None


class HyrexQueuePattern(BaseModel):
    pattern: str
    concurrency: int | None
    # concurrent_on()
