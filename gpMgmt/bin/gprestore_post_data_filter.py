#!/usr/bin/env python

from gppylib.gpparseopts import OptParser, OptChecker
from gppylib.operations.backup_utils import smart_split, checkAndRemoveEnclosingDoubleQuote, removeEscapingDoubleQuoteInSQLString
import re
import sys

search_path_expr = 'SET search_path = '
set_start = 'S'
len_search_path_expr = len(search_path_expr)
set_expr = 'SET '

comment_start_expr = '-- '
comment_expr = '-- Name: '
comment_data_expr = '-- Data: '
type_expr = '; Type: '
schema_expr = '; Schema: '
len_start_comment_expr = len(comment_start_expr)

command_start_expr = 'CREATE '
len_command_start_expr = len(command_start_expr)

def get_type(line):
    temp = line.strip()
    type_start = find_all_expr_start(temp, type_expr)
    schema_start = find_all_expr_start(temp, schema_expr)
    if len(type_start) != 1 or len(schema_start) != 1:
        return None
    type = temp[type_start[0] + len(type_expr) : schema_start[0]]
    return type

def find_all_expr_start(line, expr):
    """
    Find all overlapping matches
    """
    return [m.start() for m in re.finditer('(?=%s)' % expr, line)]

def process_schema(dump_schemas, dump_tables, fdin, fdout):
    schema = None
    type = None
    schema_buff = ''
    output = False
    further_investigation_required = False
    search_path = False
    line_buff = None
    for line in fdin:
        if (line[0] == set_start) and line.startswith(search_path_expr):
            output = False
            further_investigation_required = False
            schema = extract_schema(line)
            schema = removeEscapingDoubleQuoteInSQLString(schema, False)
            if schema in dump_schemas:
                search_path = True
                schema_buff = line
        elif (line[0] == set_start) and line.startswith(set_expr):
            output = True
        elif line[:3] == comment_start_expr and line.startswith(comment_expr):
            type = get_type(line)
            output = False
        elif schema in dump_schemas and type and (line[:7] == 'CREATE ' or line[:8] == 'REPLACE '):
            if type == 'RULE':
                output = check_table(schema, line, ' TO ', dump_tables)
            elif type == 'INDEX':
                output = check_table(schema, line, ' ON ', dump_tables)
            elif type == 'TRIGGER':
                line_buff = line
                further_investigation_required = True
        elif schema in dump_schemas and type and type in ['CONSTRAINT', 'FK CONSTRAINT'] and line[:12] == 'ALTER TABLE ':
            if line.startswith('ALTER TABLE ONLY'):
                output = check_table(schema, line, ' ONLY ', dump_tables)
            else:
                output = check_table(schema, line, ' TABLE ', dump_tables)
        elif further_investigation_required:
            if type == 'TRIGGER':
                output = check_table(schema, line, ' ON ', dump_tables)
                further_investigation_required = False

        if output:
            if search_path:
                fdout.write(schema_buff)
                schema_buff = None
                search_path = False
            if line_buff:
                fdout.write(line_buff)
                line_buff = None
            fdout.write(line)

# Given a line like 'ALTER TABLE ONLY tablename\n' and a search_str like ' ONLY ',
# extract everything between the search_str and the next space or the end of the string, whichever comes first.
def check_table(schema, line, search_str, dump_tables):
    try:
        comp_set = set()
        start = line.index(search_str) + len(search_str)
        # table name with special chars is double quoted, so looking for last double quote as ending index
        end = line.rindex('"')
        if end > 0:
            table = line[start:end+1]
        else:
            end = line.find(' ',start)
            if end > 0:
                table = line[start:end]
            else:
                table = line[start:].strip()
        table = checkAndRemoveEnclosingDoubleQuote(table)
        table = removeEscapingDoubleQuoteInSQLString(table, False)
        comp_set.add((schema, table))
        if comp_set.issubset(dump_tables):
            return True
        return False
    except:
        return False

def get_table_schema_set(filename):
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

if __name__ == "__main__":
    parser = OptParser(option_class=OptChecker)
    parser.remove_option('-h')
    parser.add_option('-h', '-?', '--help', action='store_true')
    parser.add_option('-t', '--tablefile', type='string', default=None)
    (options, args) = parser.parse_args()
    if not options.tablefile:
        raise Exception('-t table file name has to be specified')
    (schemas, tables) = get_table_schema_set(options.tablefile)
    process_schema(schemas, tables, sys.stdin, sys.stdout)

