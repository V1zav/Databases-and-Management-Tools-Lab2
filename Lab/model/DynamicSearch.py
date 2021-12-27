#!/usr/bin/env python
import Lab.utils
import datetime
import itertools
import collections
import Lab.utils.psql_types

__all__ = [
	"CompareConstant",
	"SearchCriterias",
	"SelectCompositor",
	"DynamicSearchBase",
]


class CompareConstant(object):
	def __init__(self, psql_type, comparator=None, constant=None):
		super().__init__()
		self.comparator = comparator
		self._constant = None
		self._psql_type = psql_type

	def __str__(self):
		if self.isIgnored:
			return f"""ignored"""
		# if isinstance(self.constant, str) else self.constant}
		return f"""{self.comparator} {self.constant}::{self.psql_type}"""

	def __repr__(self):
		return f"""{type(self).__name__}(comparator={self.comparator}, constant={self.constant})"""

	def reset(self):
		self.comparator = None
		self.setNull()

	def setNull(self):
		self.constant = None

	def setConstant(self, constant=None):
		if constant is None:
			return Lab.utils.menuInput(self.setConstant, [collections.namedtuple("instances", ["column_name", "data_type", "default"])(self.psql_type, self.psql_type, lambda: None)])
		else:
			self.constant = constant[next(a for a in constant if a.column_name in [self.psql_type])]
		# self.constant = click.prompt(self.psql_type, type=Lab.utils.psql_types.psql_types_convert[self.psql_type].type, default=Lab.utils.psql_types.psql_types_convert[self.psql_type].default(), show_default=True)

	@property
	def isIgnored(self):
		return self.comparator is None

	@property
	def psql_type(self):
		return self._psql_type

	@property
	def constant(self):
		if isinstance(self._constant, (str, datetime.datetime,)):
			return f"'{self._constant}'"
		elif self._constant is None:
			return f"NULL"
		# print(type(self._constant))
		return self._constant

	@constant.setter
	def constant(self, value):
		self._constant = value

	def _lt(self):
		self.comparator = "<"

	def _le(self):
		self.comparator = "<="

	def _eq(self):
		self.comparator = "="

	def _ne(self):
		self.comparator = "!="

	def _ge(self):
		self.comparator = ">="

	def _gt(self):
		self.comparator = ">"

	def _like(self):
		self.comparator = "LIKE"

	@property
	def promt(self) -> str:
		return f"Criteria editor: {self}"

	@property
	def __lab_console_interface__(self):
		result = Lab.utils.LabConsoleInterface({
			"ignore": self.reset,
			"<": self._lt,
			"<=": self._le,
			"=": self._eq,
			"!=": self._ne,
			">=": self._ge,
			">": self._gt,
			"LIKE": self._like,
			# "IS": lambda: setattr(self, "comparator", "IS"),
			# "IS NOT": lambda: setattr(self, "comparator", "IS NOT"),
			"set NULL": self.setNull,
			"set constant": self.setConstant,
			# "moar": lambda: self,
			"return": lambda: Lab.utils.menuReturn(f"User menu return"),
		}, promt=self.promt)
		return result


class SearchCriterias(list):
	def __init__(self, psql_mapping: str, psql_name: str, psql_type: str, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._psql_mapping = psql_mapping
		self._psql_name = psql_name
		self._psql_type = psql_type

	@property
	def psql_mapping(self):
		return self._psql_mapping

	@property
	def psql_name(self):
		return self._psql_name

	@property
	def psql_type(self):
		return self._psql_type

	def reset(self):
		self.clear()

	def append(self):
		# if isinstance(obj, CompareConstant):
		# 	return super().append(obj)
		# elif obj is None:
		# 	return super().append(obj)
		# raise TypeError(f"{type(obj)} is invalid")CompareConstant(self.psql_type)
		# q = CompareConstant(self.psql_type)

		try:
			next(a for a, b in enumerate(self) if b.isIgnored)
		except StopIteration:
			super().append(CompareConstant(self.psql_type))

		return self

	def gen_sql(self):
		result = f"""{" AND ".join(f"{self.psql_mapping} {a}" for a in self if not a.isIgnored)}"""
		# print(f"{result=}")
		if result:
			result = f"({result})"
		return result

	@property
	def sql(self):
		return self.gen_sql()

	def __format__(self, format_=None):
		if format_ == "v":
			return f"{list(filter(lambda x: not (x.isIgnored), self))}"
		elif format_ == "sql":
			return self.gen_sql()
		elif format_ == "pre":
			result = f"""{" AND ".join(f"{a}" for a in self if not a.isIgnored)}"""
			if result:
				return result
			return f"ignored"
		return super().__format__(format_)

	# def append_if_needed(self):
	# @property
	# def __lab_console_interface__(self):
	# 	result = Lab.utils.LabConsoleInterface()
	# 	result.update({f"Property {a} {b}": (lambda x: lambda: x)(b) for a, b in enumerate(self, 1)})
	# 	result.promt = f"{self}"
	# 	return result


class SelectCompositor(object):
	def __init__(self, search_criterias, table):
		super().__init__()
		self._search_criterias: SearchCriterias[CompareConstant] = search_criterias
		self._table = table
		self.search_criterias.append()

	@property
	def table(self):
		return self._table

	@property
	def search_criterias(self):
		return self._search_criterias

	@property
	def promt(self):
		return f'"{self.table}" {self.search_criterias:pre} select criterias:'
	

	@property
	def __lab_console_interface__(self):
		try:
			self.search_criterias.append()
			result = Lab.utils.LabConsoleInterface({
				**{f"Property {a} {b}": (lambda x: lambda: x)(b) for a, b in enumerate(self.search_criterias, 1)},
				# "new criteria": lambda: self.search_criterias[self.table].append(),
				"return": lambda: Lab.utils.menuReturn(f"User menu return"),
			}, promt=self.promt)
			return result
		except Exception as e:
			print(e)

	def __bool__(self):
		return bool(self.search_criterias)

	# def reset(self):
	# 	self.search_criterias.reset()

	# def __format__(self, *args, **kwargs):
	# 	return self.search_criterias.__format__(*args, **kwargs)


class DynamicSearchBase(object):
	def __init__(self, schema):
		super().__init__()
		self.name = type(self).__name__
		self.schema = schema
		self._search: dict[SelectCompositor] = dict()
		# self.selectcompositors = tuple()

	@property
	def search(self) -> dict[SelectCompositor]:
		return self._search

	@search.setter
	def search(self, value: dict):
		self._search = dict(itertools.starmap(lambda key, value: (key, SelectCompositor(value, key),), value.items()))

	def execute(self) -> Lab.utils.TablePrint:
		return self.schema.showData(sql=self.sql)

	def reset(self) -> None:
		for a in self.search.values():
			a.search_criterias.reset()

	@property
	def where(self) -> str:
		newline = " AND \n"
		return newline.join(f"{a.search_criterias:sql}" for a in self.search.values() if f"{a.search_criterias:sql}")

	@property
	def sql(self) -> str:
		raise NotImplementedError(f"Need to override")

	@property
	def promt(self):
		newline = f"\n"
		return f"""{self.name} dynamic search interface\n{newline.join(f'"{a}" {b.search_criterias:pre}' for a, b in self.search.items())}"""

	@property
	def __lab_console_interface__(self):
		try:
			result = Lab.utils.LabConsoleInterface({
				# **{f"{a}": (lambda x: lambda: SelectCompositor(self.search[x], x))(a) for a in self.search},
				**{a: (lambda x: lambda: x)(b) for a, b in self.search.items()},
				f"execute": self.execute,
				f"sql": lambda: print(self.sql),
				f"reset": self.reset,
				f"return": lambda: Lab.utils.menuReturn(f"User menu return"),
			}, promt=self.promt)
			return result
		except Exception as e:
			print(e)



def _test():
	pass


if __name__ == "__main__":
	_test()
