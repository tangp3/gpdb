#!/usr/bin/env python

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.operations.backup_utils import smart_split, checkAndRemoveEnclosingDoubleQuote, removeEscapingDoubleQuoteInSQLString,\
                                            escapeDoubleQuoteInSQLString, checkAndAddEnclosingDoubleQuote 
import re
import os
import sys

search_path_expr = 'SET search_path = '
set_start = 'S'
len_search_path_expr = len(search_path_expr)
copy_expr = 'COPY ' 
copy_start = 'C'
copy_expr_end = 'FROM stdin;\n'
len_copy_expr = len(copy_expr)
copy_end_expr = '\\.'
copy_end_start = '\\'
set_expr = 'SET '
drop_start = 'D'
drop_expr = 'DROP '

comment_start_expr = '-- '
comment_expr = '-- Name: '
type_expr = '; Type: '
schema_expr = '; Schema: '
owner_expr = '; Owner: '
comment_data_expr_a = '-- Data: '
comment_data_expr_b = '-- Data for Name: '
len_start_comment_expr = len(comment_start_expr)


def get_table_info(line):
    """
    It's complex to split when table name/schema name/user name/ tablespace name
    contains full expression of one of others', which is very unlikely, but in
    case it happens, return None.

    Since we only care about table name, type, and schema name, strip the input
    is safe here.

    line: contains the true (un-escaped) schema name, table name, and user name.
    """
    temp = line.strip()
    type_start = find_all_expr_start(temp, type_expr)
    schema_start = find_all_expr_start(temp, schema_expr)
    owner_start = find_all_expr_start(temp, owner_expr)
    if len(type_start) != 1 or len(schema_start) != 1 or len(owner_start) != 1:
        return (None, None, None)
    name = temp[len(comment_expr) : type_start[0]]
    type = temp[type_start[0] + len(type_expr) : schema_start[0]]
    schema = temp[schema_start[0] + len(schema_expr) : owner_start[0]]
    return (name, type, schema)

def find_all_expr_start(line, expr):
    """
    Find all overlapping matches
    """
    return [m.start() for m in re.finditer('(?=%s)' % expr, line)]

def process_schema(dump_schemas, dump_tables, fdin, fdout, change_schema=None):
    """
    Filter the dump file line by line from restore
    dump_schemas: set of schemas to restore
    dump_tables: set of (schema, table) tuple to restore
    fdin: stdin from dump file
    fdout: to write filtered content to stdout
    change_schema: different schema name to restore
    """

    schema, table = None, None
    line_buff = ''

    # to help decide whether or not to filter out
    output = False

    # to help exclude SET clause within a function's ddl statement
    function_ddl = False

    further_investigation_required = False
    search_path = True
    passedDropSchemaSection = False
    for line in fdin:
        if search_path and (line[0] == set_start) and line.startswith(search_path_expr):
            further_investigation_required = False
            # schema of set search_path is escaped in dump file
            schema = extract_schema(line)
            if removeEscapingDoubleQuoteInSQLString(schema, False) in dump_schemas:
                if change_schema:
                    line = line.replace(schema, escapeDoubleQuoteInSQLString(change_schema, False))
                output = True
                search_path = False
            else:
                output = False
        elif (line[0] == set_start) and line.startswith(set_expr) and not function_ddl:
            output = True
        elif (line[0] == drop_start) and line.startswith(drop_expr):
            if line.startswith('DROP TABLE') or line.startswith('DROP EXTERNAL TABLE'):
                if passedDropSchemaSection:
                    output = False
                else:
                    output = check_dropped_table(line, dump_tables)
            else:
                output = False
        elif line[:3] == comment_start_expr and line.startswith(comment_expr):
            # Parse the line using get_table_info for SCHEMA relation type as well,
            # if type is SCHEMA, then the value of name returned is schema's name, and returned schema is represented by '-'
            name, type, schema = get_table_info(line)
            output = False
            function_ddl = False
            passedDropSchemaSection = True
            if type in ['TABLE', 'EXTERNAL TABLE']:
                further_investigation_required = False
                output = check_valid_table(schema, name, dump_tables)
                if output:
                    search_path = True
            elif type in ['CONSTRAINT']:
                further_investigation_required = True
                if schema in dump_schemas:
                    line_buff = line 
            elif type in ['ACL']:
                output = check_valid_table(schema, name, dump_tables)
                if output:
                    search_path = True
            elif type in ['SCHEMA']:
                output = check_valid_schema(name, dump_schemas)
                if output:
                    search_path = True
            elif type in ['FUNCTION']:
                function_ddl = True
        elif (line[:3] == comment_start_expr) and (line.startswith(comment_data_expr_a) or line.startswith(comment_data_expr_b)):
            passedDropSchemaSection = True
            further_investigation_required = False
            name, type, schema = get_table_info(line)
            if type == 'TABLE DATA':
                output = check_valid_table(schema, name, dump_tables)
                if output:
                    search_path = True
            else:
                output = False  
        elif further_investigation_required:
            if line.startswith('ALTER TABLE'):
                further_investigation_required = False
                # Get the full qualified table name with the correct split
                full_table_name = line.split()[2]
                schemaname, tablename = smart_split(full_table_name)
                schemaname, tablename = checkAndRemoveEnclosingDoubleQuote(schemaname), checkAndRemoveEnclosingDoubleQuote(tablename)
                schemaname, tablename = removeEscapingDoubleQuoteInSQLString(schemaname, False), removeEscapingDoubleQuoteInSQLString(tablename, False)
                output = check_valid_table(schemaname, tablename, dump_tables)
                if output:
                    if line_buff:
                        fdout.write(line_buff)
                        line_buff = ''
                    search_path = True
        else:
            further_investigation_required = False


        if output:
            fdout.write(line)

def check_valid_schema(name, dump_schemas):
    if name in dump_schemas:
        output = True
    else:
        output = False
    return output

def check_valid_table(schema, name, dump_tables):
    """
    check if table is valid (can be from schema level restore)
    """

    if (schema, name) in dump_tables or (schema, '*') in dump_tables:
        output = True
    else:
        output = False
    return output

def get_table_schema_set(filename):
    """
    filename: file with true schema and table name (none escaped)
    """
    dump_schemas = set()
    dump_tables = set()

    with open(filename) as fd:
        contents = fd.read()
        tables = contents.splitlines()
        for t in tables:
            schema, table = smart_split(t)
            schema = checkAndRemoveEnclosingDoubleQuote(schema.strip())
            table = checkAndRemoveEnclosingDoubleQuote(table.strip())
            dump_tables.add((schema, table))
            dump_schemas.add(schema)
    with open('/tmp/save', 'w') as fw:
        fw.write('schemas are %s' % dump_schemas)
        fw.write('tables are %s' % dump_tables)
    return (dump_schemas, dump_tables)

def extract_schema(line):
    """
    Instead of searching ',' in forwarding way, search ', pg_catalog;' 
    reversely, in case schema name contains comma.

    Remove enclosing double quotes only, in case quote is part of the
    schema name
    """
    temp = line[len_search_path_expr:]
    idx = temp.rfind(", pg_catalog;")
    if idx == -1:
        return None
    schema = temp[:idx]
    return checkAndRemoveEnclosingDoubleQuote(schema)

def extract_table(line):
    """
    Instead of looking for table name ending index based on
    empty space, find it in the reverse way based on the ' ('
    whereas the column definition starts.
    
    Removing the enclosing double quote only, don't do strip('"') in case table name has double quote
    """
    temp = line[len_copy_expr:]
    idx = temp.rfind(" (")
    if idx == -1:
        return None
    table = temp[:idx]
    return checkAndRemoveEnclosingDoubleQuote(table)

def check_dropped_table(line, dump_tables):
    """
    check if table to drop is valid (can be dropped from schema level restore)
    """
    temp = line.split()[-1][:-1]
    (schema, table) = temp.split('.')
    if (schema, table) in dump_tables or (schema, '*') in dump_tables:
        return True
    return False

def process_data(dump_schemas, dump_tables, fdin, fdout, change_schema=None):
    schema, table, schema_wo_escaping = None, None, None
    output = False
    #PYTHON PERFORMANCE IS TRICKY .... THIS CODE IS LIKE THIS BECAUSE ITS FAST
    for line in fdin:
        if (line[0] == set_start) and line.startswith(search_path_expr):
            schema = extract_schema(line)
            schema_wo_escaping = removeEscapingDoubleQuoteInSQLString(schema, False)
            if schema and schema_wo_escaping in dump_schemas:
                if change_schema:
                    line = line.replace(schema, escapeDoubleQuoteInSQLString(change_schema, False))
                else:
                    schema = schema_wo_escaping
                fdout.write(line)
        elif (line[0] == copy_start) and line.startswith(copy_expr) and line.endswith(copy_expr_end):
            table = extract_table(line)
            table = removeEscapingDoubleQuoteInSQLString(table, False)
            if table and ((schema_wo_escaping, table) in dump_tables or (schema_wo_escaping, '*') in dump_tables):
                output = True
        elif output and (line[0] == copy_end_start) and line.startswith(copy_end_expr):
            table = None
            output = False
            fdout.write(line)

        if output:
            fdout.write(line)


if __name__ == "__main__":
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-t', '--tablefile', type='string', default=None)
    parser.add_option('-m', '--master_only', action='store_true')
    parser.add_option('-c', '--change_schema_file', type='string', default=None)
    (options, args) = parser.parse_args()
    if not options.tablefile:
        raise Exception('-t table file name has to be specified')
    (schemas, tables) = get_table_schema_set(options.tablefile)

    change_schema = None
    if options.change_schema_file:
        if not os.path.exists(options.change_schema_file):
            raise Exception('change schema file path %s does not exist' % options.change_schema_file)
        with open(options.change_schema_file, 'r') as fr:
            line = fr.read()
            change_schema = line.strip('\n')

    with open("/tmp/change_schema_in_filter", 'w') as fw:
        fw.write('changem schema is %s' % change_schema)

    if options.master_only:
        process_schema(schemas, tables, sys.stdin, sys.stdout, change_schema)
    else:
        process_data(schemas, tables, sys.stdin, sys.stdout, change_schema)

