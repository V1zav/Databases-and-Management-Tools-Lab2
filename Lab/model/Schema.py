#!/usr/bin/env python3

from . import DynamicSearch
from .AutoSchema import *


class DVD_rental_store(Schema):
	def __init__(self, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self._dynamicsearch = {a.name: a for a in [DynamicSearch.DiskDynamicSearch(self), DynamicSearch.LoanDynamicSearch(self), DynamicSearch.ClientDynamicSearch(self), ]}
		# self.reoverride()

	def reoverride(self):
		# Table override
		# self._tables.DVDrental = SchemaTable(self, f"DVD-rental")
		# self._tables.DVDdisk = SchemaTable(self, f"DVDdisk")
		# self._tables.client = SchemaTable(self, f"client")
		# self._tables.loan = SchemaTable(self, f"loan")
		pass

	def reinit(self):
		# sql = f"""
		# 	SELECT table_name FROM information_schema.tables
		# 	WHERE table_schema = '{self}';
		# """
		with self.dbconn.cursor() as dbcursor:
			# dbcursor.execute(sql)
			for a in self.refresh_tables():  # tuple(dbcursor.fetchall()):
				q = f"""DROP TABLE IF EXISTS {a} CASCADE;"""
				# print(q)
				dbcursor.execute(q)

		tables = [
			f"""CREATE SCHEMA IF NOT EXISTS "{self}";""",
			f"""CREATE TABLE "{self}"."DVD-rental" (
				"id" bigserial PRIMARY KEY,
				"address" character varying(255) NOT NULL,
				"name" character varying(255) NOT NULL,
				"owner" character varying(255) NOT NULL
			);
			""",
			f"""CREATE TABLE "{self}"."DVD-disk" (
				"id" bigserial PRIMARY KEY,
				"DVD-rental_id" bigint NOT NULL,
				"name" character varying(255) NOT NULL,
				"genre" character varying(255) NOT NULL,
				"date" timestamp with time zone NOT NULL,
				"price" money NOT NULL,

				CONSTRAINT "DVD-disk_DVD-rental_id_fkey" FOREIGN KEY ("DVD-rental_id")
					REFERENCES "{self}"."DVD-rental"("id") MATCH SIMPLE
					ON UPDATE NO ACTION
					ON DELETE CASCADE
					NOT VALID
			);
			""",
			f"""CREATE TABLE "{self}"."client" (
				"id" bigserial PRIMARY KEY,
				"name" character varying(255) NOT NULL,
				"surname" character varying(255) NOT NULL
			);
			""",
			f"""CREATE TABLE "{self}"."loan" (
				"id" bigserial PRIMARY KEY,
				"DVD-disk_id" bigint NOT NULL,
				"client_id" bigint NOT NULL,
				"date_loan" timestamp with time zone NOT NULL,
				"date_return" timestamp with time zone NOT NULL,
				"status" character varying(255) NOT NULL,
				CONSTRAINT "loan_DVD-disk_id_fkey" FOREIGN KEY ("DVD-disk_id")
					REFERENCES "{self}"."DVD-disk"("id") MATCH SIMPLE
					ON UPDATE NO ACTION
					ON DELETE CASCADE
					NOT VALID,
				CONSTRAINT "loan_client_id_fkey" FOREIGN KEY ("client_id")
					REFERENCES "{self}"."client"("id") MATCH SIMPLE
					ON UPDATE NO ACTION
					ON DELETE CASCADE
					NOT VALID
			);
			""",
		]

		with self.dbconn.cursor() as dbcursor:
			for a in tables:
				# print(a)
				dbcursor.execute(a)

		self.dbconn.commit()

		tables = self.refresh_tables()
		# print(f"tables: {tables}")

	def randomFill(self):
		getattr(self.tables, f"DVD-rental").randomFill(1_000)
		getattr(self.tables, f"DVD-disk").randomFill(1_000)
		self.tables.client.randomFill(2_000)
		self.tables.loan.randomFill(2_000)


def _test():
	pass


if __name__ == "__main__":
	_test()
