import logging
import logging.handlers as handler
import queue


class KinesisHandler:
    """
    Sends logs in batches to Kinesis Firehose

    Uses a queue to dispatch batched data to worker thread
    """

    def __init__(
        self,
        capacity: int,
        queue: queue.Queue,
        buffer: handler.BufferingHandler | None = None,
    ) -> None:
        """
        Initialize the handler with buffer size and queue
        """
        self.buffer = buffer or handler.BufferingHandler(capacity)
        self.queue = queue

    def prepare(self, records: list[logging.LogRecord]) -> list[str]:
        """
        Prepare data for queuing

        TODO: Is self.format() keeping all info? what about errors?
        """
        return [self.buffer.format(record) for record in records]

    def flush(self) -> None:
        """
        Put buffered data in queue and zap buffer.
        """
        self.buffer.acquire()
        try:
            self.queue.put(self.prepare(self.buffer.buffer))
            self.buffer.buffer = []
        finally:
            self.buffer.release()
