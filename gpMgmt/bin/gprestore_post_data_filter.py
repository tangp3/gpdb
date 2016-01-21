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

def process_schema(dump_schemas, dump_tables, fdin, fdout, change_schema_name, schema_level_restore_list):
    """
    Filter the dump file line by line from restore
    dump_schemas: set of schemas to restore
    dump_tables: set of (schema, table) tuple to restore
    fdin: stdin from dump file
    fdout: to write filtered content to stdout
    change_schema_name: different schema name to restore
    schema_level_restore_list: list of schemas to restore all tables under them
    """
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
            # schema in set search_path line is already escaped in dump file
            schema = extract_schema(line)
            if removeEscapingDoubleQuoteInSQLString(schema, False) in dump_schemas:
                if change_schema and len(change_schema) > 0:
                    # change schema name can contain special chars including white space, double quote that.
                    # if original schema name is already quoted, replaced it with quoted change schema name
                    quoted_schema = '"' + schema + '"'
                    if quoted_schema in line:
                        line = line.replace(quoted_schema, escapeDoubleQuoteInSQLString(change_schema))
                    else:
                        line = line.replace(schema, escapeDoubleQuoteInSQLString(change_schema))
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
    """
    filename: file with true schema and table name (none escaped), don't strip white space
    on schema and table name in case it's part of the name
    """
    dump_schemas = set()
    dump_tables = set()

    with open(filename) as fd:
        contents = fd.read()
        tables = contents.splitlines()
        for t in tables:
            schema, table = smart_split(t)
            schema = checkAndRemoveEnclosingDoubleQuote(schema)
            table = checkAndRemoveEnclosingDoubleQuote(table)
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
    parser.add_option('-c', '--change-schema-file', type='string', default=None)
    parser.add_option('-s', '--schema-level-file', type='string', default=None)
    (options, args) = parser.parse_args()
    if not (options.tablefile or options.schema_level_file):
        raise Exception('-t table file name or -s schema level file name must be specified')
    elif options.schema_level_file and options.change_schema_file:
        raise Exception('-s schema level file option can not be specified with -c change schema file option')

    (schemas, tables) = get_table_schema_set(options.tablefile)

    change_schema_name = None
    if options.change_schema_file:
        change_schema_name = get_change_schema_name(options.change_schema_file)

    schema_level_restore_list = None
    if options.schema_level_file:
        schema_level_restore_list = get_schema_level_restore_list(options.schema_level_file)
    process_schema(schemas, tables, sys.stdin, sys.stdout, change_schema_name, schema_level_restore_list)
