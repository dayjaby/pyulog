#! /usr/bin/env python

"""
Convert a ULog file into (Postgre)SQL schema
"""

from __future__ import print_function

import argparse
import os
import re

from .core import ULog

#pylint: disable=too-many-locals, invalid-name, consider-using-enumerate

def main():
    """Command line interface"""

    parser = argparse.ArgumentParser(description='Convert ULog to CSV')
    parser.add_argument('-f', '--files', nargs='+', metavar='file.ulg', help='ULog input file(s)', required=True)

    parser.add_argument('-o', '--output', dest='output', action='store',
                        help='Output file (default is same as input file)')
    args = parser.parse_args()

    convert_ulog2db(args.files, args.output)


def convert_ulog2db(ulog_file_names, output, disable_str_exceptions=False):
    """
    Converts and ULog file to a (Postgre)SQL schema file.

    :param ulog_file_name: The ULog filename to open and read
    :param output: Output file path

    :return: None
    """

    msg_filter = None

    done = dict()

    with open(output, 'w') as sqlfile:
        for ulog_file_name in ulog_file_names:
            print(ulog_file_name)
            ulog = ULog(ulog_file_name, msg_filter, disable_str_exceptions)
            data = ulog.data_list

            for d in data:
                # use same field order as in the log, except for the timestamp

                fields = [f for f in d.field_data if f.field_name == "timestamp"] + [f for f in d.field_data if f.field_name != "timestamp"]
                tablename = "ulg_{}".format(d.name)
                if "land_detected" in tablename:
                    for i in range(len(d.data['timestamp'])):
                        field_values = []
                        for k in range(len(fields)):
                            field_values.append(d.data[fields[k].field_name][i].item())
                            if fields[k].type_str == "bool":
                                field_values[-1] = bool(field_values[-1])
                        print(field_values)
                if tablename in done:
                    continue
                done[tablename] = fields
                

                # Uncomment to drop all tables:
                # sqlfile.write("DROP TABLE IF EXISTS {};\n".format(tablename))
                # continue

                # write the header
                sqlfile.write("""
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = '{}';")
    columns = cur.fetchall()

    if len(columns) == 0:
        cur.execute(\"CREATE TABLE IF NOT EXISTS {}(\"\n""".format(tablename, tablename))
                create_table_fields = ["flight_log_id TEXT, "]
                for f in fields:
                    field_name = f.field_name.replace("[", "_").replace("]", "").replace(".", "__")
                    if "padding0" in field_name:
                        continue
                    create_table_field = "{} ".format(field_name)
                    if f.type_str == "float":
                        create_table_field += "FLOAT(32)"
                    elif f.type_str == "double":
                        create_table_field += "FLOAT(53)"
                    elif f.type_str == "uint64_t":
                        create_table_field += "BIGSERIAL"
                    elif f.type_str == "uint32_t":
                        create_table_field += "SERIAL"
                    elif f.type_str == "uint16_t" or f.type_str == "uint8_t":
                        create_table_field += "SMALLSERIAL"
                    elif f.type_str == "bool":
                        create_table_field += "BOOLEAN"
                    elif f.type_str == "int64_t":
                        create_table_field += "BIGINT"
                    elif f.type_str == "int32_t":
                        create_table_field += "INTEGER"
                    elif f.type_str == "int16_t" or f.type_str == "int8_t":
                        create_table_field += "SMALLINT"
                    elif f.type_str == "char":
                        create_table_field += "TEXT"
                    else:
                        print("unhandled type: ", f.type_str)
                    create_table_fields.append(create_table_field + ", ")
                create_table_fields.append("PRIMARY KEY (flight_log_id, timestamp));")
                create_table_fields = ["""            \"{}\"""".format(x) for x in create_table_fields]
                sqlfile.write("\n".join(create_table_fields))
                sqlfile.write("\n       \"CREATE INDEX {}_timestamp_idx ON {} USING btree (timestamp ASC);\"".format(tablename, tablename));
                sqlfile.write(")\n\n")
