from pydantic import BaseModel


class HyrexQueue(BaseModel):
    name: str
    concurrency_limit: int | None = None

    def equals(self, other_queue: "HyrexQueue") -> bool:
        # TODO
        pass


class HyrexQueuePattern(BaseModel):
    pattern: str
    concurrency_limit: int | None = None
