#!/usr/bin/env python
# import itertools
# import more_itertools
import operator
import numpy
import sys
from . import psql_types

__all__ = [
	f"menuNop",
	f"menuReturn",
	f"menuReload",
	f"menuInput",

	f"TablePrint",
	f"print_console_table",
	f"fetchall_table",

	f"LabConsoleInterface",
	f"lab_console_interface",
	f"LabConsoleInterfaceDynamicUpdate",
]


class menuNop(KeyboardInterrupt):
	pass


class menuReturn(KeyboardInterrupt):
	pass


class menuReload(object):
	pass


class menuInput(object):
	def __init__(self, func, data: dict[str, psql_types.psql_types_convert_value]):
		super().__init__()
		self._func = func
		self._data: dict[str, type] = data

	@property
	def func(self):
		return self._func

	@property
	def data(self):
		return self._data

	def __iter__(self):
		return iter(self.data)


class TablePrint(object):
	__slots__ = ("table", "rowcount", "executiontime",)

	def __init__(self, table=None, rowcount: int = 0, executiontime=0):
		super().__init__()
		self.table = table
		self.rowcount = rowcount
		self.executiontime = executiontime

	def __str__(self):
		return f"{self.rowcount} rows, execution time: {self.executiontime}"


def make_equal_len(args, aggregator, side=1):
	"""makes iterable arguments same len"""

	def f(a):
		return ([aggregator, ] if type(a) != str else str(aggregator)) * (max(len(a) for a in args) - len(a))

	def g(a):
		if type(a) == tuple:
			a = list(a)
		return a + f(a) if side else f(a) + a
	return tuple(g(a) for a in args)


def print_console_table_generator(table, colum_stick=[]):
	tmp = numpy.vectorize(len)(table)
	comumn_sizes = tuple(tmp[:, a].max() for a in range(tmp.shape[1]))
	colum_stick, _ = make_equal_len((colum_stick, comumn_sizes), '<')
	for a in range(tmp.shape[0]):
		tmp = "  | ".join('{:%s%i}' % (b, a) for a, b in zip(comumn_sizes, colum_stick))
		# print(tmp.format(*table[a]))
		yield tmp.format(*table[a])


def print_console_table(table, colum_stick=list(), tab_level=0, file=sys.stdout):
	table = numpy.vectorize(str)(table)
	for a in print_console_table_generator(table, colum_stick=colum_stick):
		print('\t' * tab_level, a, sep="", file=file)


def fetchall_table(cursor):
	result: numpy.ndarray = numpy.empty([cursor.rowcount + 1, len(cursor.description), ], dtype=object)
	result[0, :] = tuple(map(operator.itemgetter(0), cursor.description))
	for ai, a in enumerate(cursor.fetchall(), 1):
		result[ai, :] = a
	return result


# def fetchall_table_print(cursor):
# 	q = TablePrint()
# 	q.table = fetchall_table(cursor)
# 	if len(result)<=0:
# 		print(f"Empty table")
	
# 	print_console_table()


class LabConsoleInterface(dict):
	def __init__(self, *args, **kwargs):
		if f"promt" in kwargs:
			self._promt = kwargs["promt"]
			del kwargs["promt"]
		else:
			self._promt = str()
		self._choices = self
		
		super().__init__(*args, **kwargs)

	@property
	def promt(self):
		return self._promt

	@promt.setter
	def promt(self, value):
		# print(f"PROMT SET {value}")
		self._promt = value

	@property
	def __lab_console_interface__(self):
		return self


class LabConsoleInterfaceDynamicUpdate(object):
	def __init__(self, menu_func, *args):
		self._menu_func = menu_func
		self._funcs = args

	@property
	def menu_func(self):
		return self._menu_func

	@property
	def funcs(self):
		return self._funcs

	@property
	def  __lab_console_interface__(self):
		for a in self._funcs:
			a()
		return self.menu_func()


def lab_console_interface(obj):
	try:
		result = obj.__lab_console_interface__
		if isinstance(result, dict):
			pass
		else:
			print(dir(obj))
			raise TypeError(f"{type(obj)} {obj} is not supported")
	except AttributeError as e:
		raise e
	return result

def _test() -> None:
	pass


if __name__ == "__main__":
	_test()
