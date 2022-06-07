# this one complies to source_spec.json defined in root of the python connector
import enum
import re


class SourceParams:
    def __init__(
            self,
            *,
            bucket,
            aws_access_key_id,
            aws_secret_access_key,
            prefix,
            filter_regexp,
            partitioning_type,
            plain_tables=None,
            file_type
    ):
        self.bucket = bucket
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.prefix = prefix
        self.filter_regexp = filter_regexp
        self.filter_re = re.compile(filter_regexp)

        if partitioning_type == "plain_tables":
            self.partitioning_type = PartitioningType.PLAIN_TABLES
        else:
            raise ValueError(f"invalid value of field 'partitioning_type': ${file_type}")
        self.plain_tables = plain_tables
        if file_type == "csv":
            self.file_type = FileType.CSV
        elif file_type == "json_lines":
            self.file_type = FileType.JSON_LINES
        else:
            raise ValueError(f"invalid value of field 'plain_tables': ${file_type}")


class PartitioningType(enum.Enum):
    UNKNOWN = 0
    PLAIN_TABLES = 1

class FileType(enum.Enum):
    UNKNOWN = 0
    CSV = 1
    JSON_LINES = 2
