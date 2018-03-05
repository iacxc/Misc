#!/usr/bin/python -O

from xlrd import open_workbook
from xlutils.copy import copy
import sqlite3
from datetime import date, time

def to_str(value):
    if isinstance(value, unicode):
        return value.encode("utf-8")
    else:
        return str(value)


xls_file = "C.xlsx"

rb = open_workbook(xls_file)

sheet = rb.sheet_by_name("Sheet1")

conn = sqlite3.connect("workbook.db")
cursor = conn.cursor()
cursor.execute("delete from sheet1")

for row in list(sheet.get_rows())[1:]:
    print row
    cursor.execute("insert into sheet1 values (?,?,?,?,?,?)",
                   (row[0].value, row[1].value, row[2].value, 
                    row[3].value, row[4].value, row[5].value))

    print

conn.commit()

#wb = copy(rb)
