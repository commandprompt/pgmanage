'''
The MIT License (MIT)

Copyright (c) 2014-2019 William Ivanski

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
'''

import os
import csv
import openpyxl
from collections import OrderedDict
import tempfile
import json
from django.core.serializers.json import DjangoJSONEncoder
import app.include.Spartacus as Spartacus

class Exception(Exception):
    pass


class DataFileWriter(object):
    def __init__(self, filename, fieldnames=None, encoding='utf-8', delimiter=';', lineterminator='\n', skip_headers=False):
        tmp = filename.split('.')
        if len(tmp) > 1:
            self.extension = tmp[-1].lower()
        else:
            self.extension = 'csv'
        if self.extension == 'txt' or self.extension == 'out':
            self.extension = 'csv'

        self.filename = filename
        self.file_handle = None
        self.header = []
        self.currentrow = 1
        self.opened = False
        self.writer = None
        self.encoding = encoding
        self.delimiter = delimiter
        self.lineterminator = lineterminator
        self.skip_headers = skip_headers
        for idx, field in enumerate(fieldnames):
            if field == '?column?':
                self.header.append(f'?column-{idx}')
            else:
                self.header.append(field)

    def Open(self):
        try:
            if self.extension == 'csv':
                self.file_handle = open(self.filename, 'w', encoding=self.encoding)
                self.writer = csv.writer(self.file_handle, delimiter=self.delimiter, lineterminator=self.lineterminator)
                if not self.skip_headers:
                    self.writer.writerow(self.header)
                self.opened = True
            elif self.extension == 'xlsx':
                self.writer = openpyxl.Workbook(write_only=True)
                self.opened = True
            elif self.extension == 'json':
                self.file_handle = open(self.filename, 'w+', encoding=self.encoding)
                print("[", file=self.file_handle, end='')
                self.opened = True
            else:
                raise Spartacus.Utils.Exception('File extension "{0}" not supported.'.format(self.extension))
        except Spartacus.Utils.Exception as exc:
            raise exc
        except Exception as exc:
            raise Spartacus.Utils.Exception(str(exc))
    def Write(self, p_datatable, p_hasmore=False):
        try:
            if not self.opened:
                raise Spartacus.Utils.Exception('You need to call Open() first.')
            if self.extension == 'csv':
                for row in p_datatable.Rows:
                    self.writer.writerow(row)
            elif self.extension == 'xlsx':
                if self.currentrow == 1:
                    worksheet = self.writer.create_sheet()
                    if not self.skip_headers:
                        worksheet.append(p_datatable.Columns)
                        self.currentrow = self.currentrow + 1
                else:
                    worksheet = self.writer.active
                for r in range(0, len(p_datatable.Rows)):
                    row = []
                    for c in range(0, len(p_datatable.Columns)):
                        row.append(p_datatable.Rows[r][c])
                    worksheet.append(row)
                self.currentrow = self.currentrow + len(p_datatable.Rows)
            else:
                for row in p_datatable.Rows:
                    print(
                        json.dumps(
                            dict(zip (self.header, row)), cls=DjangoJSONEncoder),
                            end=',\n',
                            file=self.file_handle
                    )

        except Spartacus.Utils.Exception as exc:
            raise exc
        except Exception as exc:
            raise Spartacus.Utils.Exception(str(exc))
    def Flush(self):
        try:
            if not self.opened:
                raise Spartacus.Utils.Exception('You need to call Open() first.')
            if self.extension == 'csv':
                self.file_handle.close()
            elif self.extension == 'xlsx':
                self.writer.save(self.filename)
            else:
                pos = self.file_handle.tell()
                # non-empty json file
                if pos > 2:
                    # rewind 2 bytes back so the last ',\n' gets truncated
                    self.file_handle.seek(pos-2)
                print("]", file=self.file_handle)
                self.file_handle.close()

        except Spartacus.Utils.Exception as exc:
            raise exc
        except Exception as exc:
            raise Spartacus.Utils.Exception(str(exc))
