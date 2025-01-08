from pydantic import BaseModel


class HyrexQueue(BaseModel):
    name: str
    concurrency_limit: int = 0

    def equals(self, other_queue: "HyrexQueue") -> bool:
        return (
            self.name == other_queue.name
            and self.concurrency_limit == other_queue.concurrency_limit
        )
