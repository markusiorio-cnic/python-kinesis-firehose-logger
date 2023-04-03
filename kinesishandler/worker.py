import logging
import boto3
import threading
import queue


class Worker:
    """
    Polls queue for next batch of log data and sends it to kinesis

    TODO:
    Each PutRecords request can support up to 500 records. Each record in the
    request can be as large as 1 MB, up to a limit of 5 MB for the entire request,
    including partition keys. Each shard can support writes up to 1,000 records per
    second, up to a maximum data write total of 1 MB per second.
    """

    _sentinel = None

    def __init__(
        self,
        queue: queue.Queue,
        streamname: str,
        boto_client: None = None,
        region=None,
    ) -> None:
        """
        Initialize the worker with queue, Kinesis stream name, AWS region and partition key.

        Specifying region is optional. If region is not specified,
        the AWS SDK will try to resolve the default region and fail if not found.

        Specifying partition key is optional. If partition key is not specified,
        it is assumed the Kinesis stream has only one shard and a constant is used as partition key.

        TODO:
        Handle partition key for each record, see comment above.
        """
        self.queue = queue
        self.firehose = boto_client or boto3.client("firehose", region)
        self.streamname = streamname
        self._stop = threading.Event()
        self._thread = None
        self.validate_stream()

    def validate_stream(self) -> None:
        """
        Validate the specified Kinesis Firehose stream

        Raises exception if the specified stream is not accessible nor active.
        """

        stream = self.firehose.describe_delivery_stream(
            DeliveryStreamName=self.streamname
        )
        if "ACTIVE" != stream["DeliveryStreamDescription"]["DeliveryStreamStatus"]:
            raise ValueError("{} is not active".format(self.streamname))

    def start(self) -> None:
        """
        Start the worker.
        """
        self._thread = threading.Thread(target=self._monitor)
        self._thread.setDaemon(True)
        self._thread.start()

    def stop(self) -> None:
        """
        Stop the worker.
        """
        self._stop.set()
        self.queue.put_nowait(self._sentinel)
        self._thread.join()
        self._thread = None

    def prepare(
        self, records: list[logging.LogRecord]
    ) -> list[dict[str, logging.LogRecord]]:
        """
        Prepare the batch of records to be sent to Kinesis.

        Data is sent as a `boto3.Kinesis.Client.put_records()` request,
        and needs to be formatted accordingly.
        """

        def fmt(record: logging.LogRecord) -> dict[str, logging.LogRecord]:
            return {"Data": record}

        return [fmt(record) for record in records]

    def handle(self, records: list[logging.LogRecord]) -> None:
        """
        Handle a record.

        Send data to Kinesis as a `boto3.Kinesis.Client.put_records()` request.
        """
        data = self.prepare(records)
        try:
            self.firehose.put_record_batch(
                Records=data, DeliveryStreamName=self.streamname
            )
        except Exception as e:
            # TODO: do something to recover here
            raise e

    def _monitor(self) -> None:
        """
        Monitor the queue for records and handle them.

        This method runs on a separate, internal thread.
        The thread will terminate if it sees a sentinel object in the queue.
        """
        q = self.queue
        has_task_done = hasattr(q, "task_done")

        # Main loop, run until stopped or sentinel is found
        while not self._stop.isSet():
            try:
                records = self.queue.get(True)
                if records is self._sentinel:
                    break
                self.handle(records)
                if has_task_done:
                    q.task_done()
            except Exception as e:
                # TODO: do something to recover here
                raise e

        # There might still be records in the queue, run until empty
        while True:
            try:
                records = self.queue.get(False)
                if records is self._sentinel:
                    break
                self.handle(records)
                if has_task_done:
                    q.task_done()
            except queue.Empty:
                break
