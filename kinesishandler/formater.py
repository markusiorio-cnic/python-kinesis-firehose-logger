import json
import logging


class SimpleJsonFormatter:
    """
    Simply JSON log formatter for Amazon Kinesis Firehose logging
    """

    def __init__(self, formatter: logging.Formatter | None = None) -> None:
        self.formatter = formatter or logging.Formatter()

    def format(self, record: logging.LogRecord) -> str:
        ret = {}
        for attr, value in record.__dict__.items():
            if attr == "asctime":
                value = self.formater.formatTime(record)
            if attr == "exc_info" and value is not None:
                value = self.formater.formatException(value)
            if attr == "stack_info" and value is not None:
                value = self.formater.formatStack(value)
            ret[attr] = value
        return json.dumps(ret)
