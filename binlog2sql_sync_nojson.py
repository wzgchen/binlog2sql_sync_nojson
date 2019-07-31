# -*- coding: UTF-8 -*-
# binlog2sql --> new binlogtosql

'''

支持python2.7和python3
pip install PyMySQL==0.7.11
pip install mysql-replication==0.13



不支持json表的dml的解析，可以支持简单类的json语法，如果超了原先包的解析，则会出错，该错误在binlog2sql也会出现
INSERT INTO t_json(id,name,info) VALUES(1 ,'test','{"time":"2017-01-01 13:00:00","ip":"192.168.1.1","result":"fail"}');
会出这样的错：ValueError: Json type 15 is not handled，该错误是为pymysqlreplication原生包报出

功能：
1.模拟从节点，用于进行数据迁移时进行增量数据同步

'''

from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from pymysqlreplication import BinLogStreamReader
import json
import pymysql
import datetime
import sys
import io
import getopt



if sys.version > '3':
    PY3PLUS = True
else:
    PY3PLUS = False


def compare_items(items):
    # caution: if v is NULL, may need to process
    (k, v) = items
    if v is None:
        return '`%s` IS %%s' % k
    else:
        return '`%s`=%%s' % k


def fix_object(value):
    """Fixes python objects so that they can be properly inserted into SQL queries"""
    if isinstance(value, set):
        value = ','.join(value)
    if PY3PLUS and isinstance(value, bytes):
        return value.decode('utf-8')
    elif not PY3PLUS and isinstance(value, unicode):
        return value.encode('utf-8')
    else:
        return value





class file_position:
    # dic = {'binlog': 'mysql-bin.000043', 'start-position': 289}

    def get(self):
        # read from local
        f = open("file_position.log", 'r')
        dic = eval(f.read())
        f.close()
        return dic

    def set(self, dic):
        # save to local
        f = open("file_position.log", 'w')
        f.write(str(dic))
        f.close()



def write_sql(sql, outfile):
    with io.open(outfile, 'a+', encoding='utf-8') as f:
        if PY3PLUS:
            f.writelines(sql + '\n')
        else:
            sql = unicode(sql, "utf-8")
            f.writelines(sql + '\n')
        print(f)


def convert(data):
    if isinstance(data, bytes):  return data.decode()
    if isinstance(data, dict):   return dict(map(convert, data.items()))
    if isinstance(data, tuple):  return tuple(map(convert, data))
    if isinstance(data, list):   return list(map(convert, data))
    return data


def parse_args():
    binlog = ""
    host = ""
    port = 3306
    user = ""
    password = ""

    start_position = ""
    database = None
    table = None
    outfile = ""
    try:
        options, args = getopt.getopt(sys.argv[1:], "f:h:u:p:P:d:t:",
                                      ["help", "host=", "user=", "password=", "port=", "binlog=", "start-position=",
                                       "database=", "table=", "outfile="])
    except getopt.GetoptError:
        print("参数输入有误!!!!!")
        options = []
    if options == [] or options[0][0] in ("--help"):
        usage()
        sys.exit()

    for name, value in options:
        if name == "-f" or name == "--binlog":
            binlog = value
        if name == "-h" or name == "--host":
            host = value
        if name == "-u" or name == "--user":
            user = value
        if name == "-p" or name == "--password":
            password = value
        if name == "-P" or name == "--port":
            port = value

        if name == "--start-position":
            start_position = value
        if name == "-d" or name == "--database":
            database = value
        if name == "-t" or name == "--table":
            table = value
        if name == "--outfile":
            outfile = value

        if binlog == '':
            print("错误:请指定binlog文件名!")
            usage()
    kw = {}
    kt = {}

    kw['binlog'] = binlog

    kw['host'] = host
    kw['user'] = user
    kw['password'] = password
    kw['port'] = port

    kw['start_position'] = start_position
    kw['database'] = database
    kw['table'] = table
    kw['outfile'] = outfile

    return kw


def usage():
    usage_info = """==========================================================================================
Command line options :
    --help                  # OUT : print help info
    -f, --binlog            # IN  : binlog file. (required:/opt/mysql/data3306/mysql-bin.000043)
    -d, --database          # IN  : specify database (No default value).
    -t, --table             # IN  : specify database (No default value).
    --start-position        # IN  : start position. (default '4')
    --outfile               # IN  : outfile. (No default value)
    
Sample :
   shell> python binlog2sql_sync_nojson_v1.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306 
   shell> python binlog2sql_sync_nojson_v1.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306 --database=test
   shell> python binlog2sql_sync_nojson_v1.py -f mysql-bin.000001 --start-position=2585 --outfile=/tmp/1.sql  --host=192.168.1.1 --user=admin --password=123456 --port=3306 --database=test --table=t1
==============================================================================:============"""

    print(usage_info)
    sys.exit()


class binlog2sql():
    def __init__(self, parser):
        self.parser = parser
        self.host = parser['host']
        self.port = int(parser['port'])
        self.user = parser['user']
        self.passwd = parser['password']

    def db_save(self, sql_template, values, log_content, outfile):
        db = pymysql.connect(host=self.host, user=self.user, password=self.passwd, database="mysql", port=self.port,
                             charset='utf8')
        cursor = db.cursor()

        # process json dml，除掉注释，可以解析简单的json语法，前提是replication协议包支持
        newlist = list(values)
        # for index, v in enumerate(newlist):
        #     if isinstance(v, dict):
        #         # newlist[index] =  json.dumps({ key.decode(): val.decode() for key, val in v.items() if not isinstance(val,int)})
        #         newlist[index] = json.dumps({convert(key): convert(val) for key, val in v.items()})
        #
        # print('newlist:', newlist)
        newsql = cursor.mogrify(sql_template, newlist)
        if not PY3PLUS:
            log_content = log_content.encode("utf-8")
            newsql += log_content
        else:
            newsql += log_content

        try:
            # cursor.execute(newsql)
            # db.commit()
            write_sql(newsql, outfile)
            print('execute ok:', newsql)
        except Exception as e:
            # 发生错误时回滚
            print('error:', e)
            db.rollback()
        # 关闭数据库连接
        db.close()

    def generate_sql(self):
        outfile = self.parser['outfile']
        database = self.parser['database']
        table = self.parser['table']
        conn_setting = {'host': parser['host'], 'port': int(parser['port']), 'user': parser['user'],
                        'passwd': parser['password'], 'charset': 'utf8'}

        log_file = parser['binlog']
        start_position = parser['start_position'] if parser['start_position'] else 4

        dict_ = {'log_file': log_file, 'log_pos': int(start_position)}
        f = file_position()
        f.set(dict_)

        fpos = f.get()
        res_file = fpos['log_file']
        res_pos = fpos['log_pos']
        print('cache_res:', res_file, res_pos)

        stream = BinLogStreamReader(
            connection_settings=conn_setting,
            only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent],
            only_schemas=database, only_tables=table,
            log_file=res_file,
            log_pos=res_pos,
            server_id=30, blocking=True, resume_stream=True)

        for binlogevent in stream:
            if isinstance(binlogevent, WriteRowsEvent):
                for row in binlogevent.rows:
                    next_binlog = stream.log_file
                    postion = stream.log_pos
                    log_timestamp = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                    start = f.get()['log_pos']
                    log_content = ' # binlog: %s start:%s end:%s time: %s' % (
                        next_binlog, str(start), str(postion), log_timestamp)

                    template = 'INSERT INTO `{0}`.`{1}`({2}) VALUES ({3});' \
                        .format(binlogevent.schema, binlogevent.table,
                                ', '.join(map(lambda key: '`%s`' % key, row['values'].keys())),
                                ', '.join(['%s'] * len(row['values'])))
                    values = map(fix_object, row['values'].values())
                    self.db_save(template, values, log_content, outfile)

                    dict_ = {'log_file': next_binlog, 'log_pos': postion}
                    f = file_position()
                    f.set(dict_)
                    print('set:  ', str(dict_))

            elif isinstance(binlogevent, DeleteRowsEvent):
                if binlogevent.primary_key:
                    for row in binlogevent.rows:
                        next_binlog = stream.log_file
                        postion = stream.log_pos
                        log_timestamp = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                        start = f.get()['log_pos']
                        log_content = ' # binlog: %s start:%s end:%s time: %s' % (
                            next_binlog, str(start), str(postion), log_timestamp)

                        prikey = binlogevent.primary_key
                        beoreprikey_items = {k: v for k, v in row['values'].items() if k in prikey}.items()
                        beoreprikey_values = [v for k, v in beoreprikey_items]

                        template = 'DELETE FROM `{0}`.`{1}` WHERE {2} LIMIT 1;'.format(binlogevent.schema,
                                                                                       binlogevent.table,
                                                                                       ' AND '.join(map(compare_items,
                                                                                                        beoreprikey_items)))
                        values = map(fix_object, beoreprikey_values)
                        self.db_save(template, values, log_content, outfile)

                        dict_ = {'log_file': next_binlog, 'log_pos': postion}
                        f = file_position()
                        f.set(dict_)
                        print('set:  ', str(dict_))

                else:
                    next_binlog = stream.log_file
                    postion = stream.log_pos
                    log_timestamp = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                    start = f.get()['log_pos']
                    log_content = ' # binlog: %s start:%s end:%s time: %s' % (
                        next_binlog, str(start), str(postion), log_timestamp)

                    for row in binlogevent.rows:
                        print('del_nopri_row:    ', binlogevent.schema, binlogevent.table, row, log_content)

                    dict_ = {'log_file': next_binlog, 'log_pos': postion}
                    f = file_position()
                    f.set(dict_)
                    print('set:  ', str(dict_))
                    print('del表没有主键,或含有JSON的表，无法处理，退出')
                    sys.exit()

            elif isinstance(binlogevent, UpdateRowsEvent):
                if binlogevent.primary_key:
                    for row in binlogevent.rows:
                        prikey = binlogevent.primary_key
                        beoreprikey_items = {k: v for k, v in row['before_values'].items() if k in prikey}.items()
                        beoreprikey_values = [v for k, v in beoreprikey_items]

                        next_binlog = stream.log_file
                        postion = stream.log_pos
                        log_timestamp = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                        start = f.get()['log_pos']
                        log_content = ' # binlog: %s start:%s end:%s time: %s' % (
                            next_binlog, str(start), str(postion), log_timestamp)

                        template = 'UPDATE `{0}`.`{1}` SET {2} WHERE {3} LIMIT 1;'.format(binlogevent.schema,
                                                                                          binlogevent.table, ', '.join(
                                ['`%s`=%%s' % k for k in row['after_values'].keys()]), ' AND '.join(
                                map(compare_items, beoreprikey_items)))
                        values = map(fix_object, list(row['after_values'].values()) + list(beoreprikey_values))
                        self.db_save(template, values, log_content, outfile)

                        dict_ = {'log_file': next_binlog, 'log_pos': postion}
                        f = file_position()
                        f.set(dict_)
                        print('set:  ', str(dict_))

                else:
                    next_binlog = stream.log_file
                    postion = stream.log_pos
                    log_timestamp = datetime.datetime.fromtimestamp(binlogevent.timestamp)
                    start = f.get()['log_pos']
                    log_content = ' # binlog: %s start:%s end:%s time: %s' % (
                    next_binlog, str(start), str(postion), log_timestamp)

                    for row in binlogevent.rows:
                        print('update_nopri_row:    ', binlogevent.schema, binlogevent.table, row, log_content)

                    dict_ = {'log_file': next_binlog, 'log_pos': postion}
                    f = file_position()
                    f.set(dict_)
                    print('set:  ', str(dict_))
                    print('update表没有主键,或含有JSON的表，无法处理，退出')
                    sys.exit()


if __name__ == '__main__':
    parser = parse_args()
    binlog2sql = binlog2sql(parser)
    binlog2sql.generate_sql()
