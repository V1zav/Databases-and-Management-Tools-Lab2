#!/usr/bin/env python

import datetime
# import click
import click_datetime
import collections

__all__ = ["psql_types_convert", "psql_types_to_random"]

psql_types_convert_value = collections.namedtuple("psql_types_convert_value", ["type", "default"])

psql_types_convert: dict[str, psql_types_convert_value] = {
	"character varying": psql_types_convert_value(str, lambda: None),
	"varchar": psql_types_convert_value(str, lambda: None),
	"bigint": psql_types_convert_value(int, lambda: None),
	"int": psql_types_convert_value(int, lambda: None),
	"integer": psql_types_convert_value(int, lambda: None),
	"money": psql_types_convert_value(float, lambda: None),
	"timestamp with time zone": psql_types_convert_value(click_datetime.Datetime(format="%Y-%m-%d"), lambda: datetime.datetime.now()),  # default=datetime.now(),%H:%M:%S
	"timestamp without time zone": psql_types_convert_value(click_datetime.Datetime(format="%Y-%m-%d"), lambda: datetime.datetime.now()),
	"timestampz": psql_types_convert_value(click_datetime.Datetime(format="%Y-%m-%d"), lambda: datetime.datetime.now()),
	"timestamp": psql_types_convert_value(click_datetime.Datetime(format="%Y-%m-%d"), lambda: datetime.datetime.now()),
	"boolean": psql_types_convert_value(bool, lambda: None),
}

psql_types_to_random: dict[str] = {
	"character varying": lambda x: f"""substr(characters, (random() * length(characters) + 1)::integer, 10)""",
	"bigint": lambda x: f"""trunc(random() * 100)::int""",
	"int": lambda x: f"""trunc(random() * 100)::int""",
	"integer": lambda x: f"""trunc(random() * 100)::int""",
	"money": lambda x: f"""trunc(random() * 100)::int""",
	"timestamp with time zone": lambda x: f"""timestamp '2021-01-01' + random() * (timestamp '2021-11-11' - timestamp '2021-01-01')""",
	"boolean": lambda x: f"""round(random()),""",
}


def _test():
	pass


if __name__ == "__main__":
	_test()
