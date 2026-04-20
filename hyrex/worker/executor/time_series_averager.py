import time
from collections import deque

from pydantic import BaseModel


class DataPoint(BaseModel):
    value: float
    timestamp: int


class MinuteAverage(BaseModel):
    minute: int  # Unix timestamp rounded to minute
    average: float


# Bound the per-averager ring buffer. The executor poll loop submits to these
# averagers on every iteration and previously never pruned, which leaked ~40
# MB/min per worker under an idle busy-poll. 10k entries is enough to hold
# several minutes of stats at realistic submit rates while capping worst-case
# memory at ~2 MB per averager.
_MAX_DATA_POINTS = 10_000


class TimeSeriesAverager:
    def __init__(self):
        self.data_points: deque[DataPoint] = deque(maxlen=_MAX_DATA_POINTS)

    def _get_minute_timestamp(self, timestamp: int) -> int:
        # Round down to nearest minute
        return (timestamp // 60000) * 60000

    def _group_by_minute(self) -> dict[int, list[DataPoint]]:
        grouped_data: dict[int, list[DataPoint]] = {}

        for point in self.data_points:
            minute = self._get_minute_timestamp(point.timestamp)
            if minute not in grouped_data:
                grouped_data[minute] = []
            grouped_data[minute].append(point)

        return grouped_data

    def submit(self, x: float) -> None:
        current_time = int(time.time() * 1000)  # Convert to milliseconds
        self.data_points.append(DataPoint(value=x, timestamp=current_time))

    def get_time_series(self) -> list[MinuteAverage]:
        grouped_data = self._group_by_minute()
        result: list[MinuteAverage] = []

        # Sort minutes chronologically
        sorted_minutes = sorted(grouped_data.keys())

        for minute in sorted_minutes:
            points = grouped_data[minute]
            avg = sum(point.value for point in points) / len(points)

            result.append(MinuteAverage(minute=minute, average=avg))

        return result

    def get_average_for_minute(self, timestamp: int) -> MinuteAverage | None:
        minute = self._get_minute_timestamp(timestamp)
        grouped_data = self._group_by_minute()

        if minute not in grouped_data:
            return None

        points = grouped_data[minute]
        avg = sum(point.value for point in points) / len(points)

        return MinuteAverage(minute=minute, average=avg)

    def get_current_minute_average(self) -> MinuteAverage:
        current_minute = self._get_minute_timestamp(int(time.time() * 1000))
        result = self.get_average_for_minute(current_minute)
        if result is None:
            return MinuteAverage(minute=current_minute, average=0)
        return result

    def clear(self) -> None:
        self.data_points.clear()

    def prune_data_older_than(self, timestamp: int) -> None:
        self.data_points = deque(
            (point for point in self.data_points if point.timestamp >= timestamp),
            maxlen=_MAX_DATA_POINTS,
        )
