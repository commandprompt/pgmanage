import logging
import re


class MaskedDataFilter(logging.Filter):
    def filter(self, record):
        # TODO: add more data masking rules
        rules = [('password":"\S*"', 'password":"[redacted]"')]

        if hasattr(record, "msg"):
            for old, new in rules:
                record.msg = re.sub(old, new, record.msg)

        return True
