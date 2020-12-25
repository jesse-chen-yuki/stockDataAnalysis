import mysql.connector
from datetime import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta
from clickhouse_driver import Client
import re
import logging
from csv import DictReader
import os
from itertools import islice
import shutil
import multivolumefile
from py7zr import SevenZipFile
import talib
import numpy


def kbar_num(stock, start, end):
    global mydb
    mycursor = mydb.cursor()

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]

    sql = '''
        SELECT count(*) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime >= ''' + start_str + '''
     and origtime <= ''' + end_str + '''
     order by applseqnum'''
    logging.debug(sql)

    mycursor.execute(sql)
    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_amount(stock, start, end):
    global mydb
    mycursor = mydb.cursor()

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]

    sql = '''
        SELECT sum(TradeQty*price) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime >= ''' + start_str + '''
     and origtime <= ''' + end_str + '''
     order by applseqnum'''
    logging.debug(sql)

    mycursor.execute(sql)
    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_volume(stock, start, end):
    global mydb
    mycursor = mydb.cursor()

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]

    sql = '''
        SELECT sum(TradeQty) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime >= ''' + start_str + '''
     and origtime <= ''' + end_str + '''
     order by applseqnum'''
    # logging.debug(sql)

    mycursor.execute(sql)
    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_open(stock, start):
    global mydb
    mycursor = mydb.cursor()

    time_str = start.strftime('%Y%m%d%H%M%S%f')
    time_str = time_str[:-3]

    # logging.debug(time_str)
    sql = '''
        SELECT price FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime>= ''' + time_str + '''
     order by applseqnum
     limit 1'''
    # logging.debug(sql)

    mycursor.execute(sql)

    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_close(stock, end):
    global mydb
    mycursor = mydb.cursor()

    time_str = end.strftime('%Y%m%d%H%M%S%f')
    time_str = time_str[:-3]

    # logging.debug(time_str)
    sql = '''
        SELECT price FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime<= ''' + time_str + '''
     order by applseqnum desc
     limit 1'''
    # logging.debug(sql)

    mycursor.execute(sql)

    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_high(stock, start, end):
    global mydb
    mycursor = mydb.cursor()

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]

    sql = '''
        SELECT max(price) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime >= ''' + start_str + '''
     and origtime <= ''' + end_str + '''
     order by applseqnum'''
    # logging.debug(sql)

    mycursor.execute(sql)
    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_low(stock, start, end):
    global mydb
    mycursor = mydb.cursor()

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]

    sql = '''
        SELECT min(price) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock + '''
     and origtime >= ''' + start_str + '''
     and origtime <= ''' + end_str + '''
     order by applseqnum'''
    # logging.debug(sql)

    mycursor.execute(sql)
    try:
        return mycursor.fetchone()[0]
    except Exception as e:
        print(e)
        return None


def kbar_seg(stock, start, end):
    # analyze the specified stock in the specified time period
    # returns following
    # start time, stock, open, high, low, close, volume, amount, num_trades, vwap

    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]

    open_amt = kbar_open(stock, start)
    high = kbar_high(stock, start, end)
    low = kbar_low(stock, start, end)
    close = kbar_close(stock, end)
    volume = kbar_volume(stock, start, end)
    amount = kbar_amount(stock, start, end)
    num_trade = kbar_num(stock, start, end)
    try:
        vwap = amount / volume
    except Exception as e:
        print(e)
        vwap = 0

    return [stock, start_str, open_amt, high, low, close, volume, amount, num_trade, vwap]


def kbar(start, end, stock_list, period):
    start_time = datetime.fromisoformat(start)
    end_time = datetime.fromisoformat(end)
    time_increment = 0
    # logging.debug(start_time)
    # logging.debug(end_time)

    result = []

    if period == 's':
        time_increment = timedelta(seconds=1)
    elif period == 'm':
        time_increment = timedelta(minutes=1)
    elif period == 'h':
        time_increment = timedelta(hours=1)
    elif period == 'd':
        time_increment = timedelta(days=1)
    elif period == 'w':
        time_increment = timedelta(weeks=1)
    elif period == 'M':
        time_increment = relativedelta(months=1)

    for stock in stock_list:
        # find out how many queries need to be sent based on start, end, period
        logging.debug('stock ' + str(stock))

        q_start = start_time
        q_end = start_time + time_increment

        while q_start <= end_time:
            # call query function
            # result.append(q_start)
            result.append(kbar_seg(stock, q_start, q_end))

            q_start += time_increment
            q_end += time_increment

        logging.debug('next set \n')

    return result


def get_dataset(start, end, stock, column):
    # input
    # start: start time format yyyymmddhhmmssxxx
    # end: start time format yyyymmddhhmmssxxx
    # stock: stock list with id format ssssss
    # column: appoint what column to get

    # query the db regarding specific stuff
    # return_value = database.get_dataset(start_time, end_time, stock_list, fields = ["price", "volume"]), 
    # return a dataset contains time, stock code, price, volume

    global mydb
    mycursor = mydb.cursor()

    # logging.debug('under test')
    # logging.debug(start, end, stock)

    sql = 'SELECT origtime, securityID '
    for a in column:
        if a == '1':
            sql = sql + ', price '
        elif a == '2':
            sql = sql + ', tradeqty '

    sql = sql + 'FROM shenzhendata.hq_trade_spot \n'
    sql = sql + ' where origtime >=  \'' + start + '\'\n'
    sql = sql + ' and origtime <=  \'' + end + '\'\n'
    sql = sql + ' and ( \n securityID =  \'' + stock.pop(0) + '\'\n'

    for a in stock:
        sql = sql + ' or securityID =  \'' + a + '\'\n'
    sql = sql + ')'

    logging.debug(sql)
    mycursor.execute(sql)

    result = []
    for x in mycursor:
        result.append(x)

    # logging.debug(result)
    mycursor.close()
    return result


def import_line(f, name):
    mycursor = mydb.cursor()
    table = name.replace('.txt', '').replace('am_', '').replace('pm_', '')
    read_count_max = 10

    # logging.debug(sql)

    i = 0
    for a in f:
        print(a)
        # read a line from file
        a = f.readline()
        # logging.debug(a)
        a = a[0:-1]
        # logging.debug(a)
        # split the line into tokens
        # token = re.split(r'\t+',a)
        token = re.split('\t+', a)
        # logging.debug(token)

        if table == 'hq_index':
            # logging.debug ('need one less element')
            token.remove('')

        sql = construct_insert_statement(table, token)
        logging.debug(sql)
        mycursor.execute(sql)

        if i == read_count_max:
            mydb.commit()
            break
        else:
            i += 1

    logging.debug("continue")

    #    for x in f:
    #        logging.debug(x)
    #
    #
    #        # read a line from file
    #        a = f.readline()
    #        logging.debug(a)
    #        # split the line into tokens
    #        token = re.split(r'\t+',a)
    #        logging.debug(token)

    #
    #    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    #    val = ("John", "Highway 21")
    #
    #    sql = "Show Tables"
    #    mycursor.execute(sql)
    #
    #
    #    #mydb.commit()
    #
    #    #logging.debug(mycursor.rowcount, "record inserted.")
    #    #logging.debug("1 record inserted, ID:", mycursor.lastrowid)
    #
    #    #mycursor.execute("SELECT * FROM customers")
    #
    #    #myresult = mycursor.fetchall()
    #
    #    for x in mycursor:
    #      logging.debug(x)


def import_line_ch(f, name):
    logging.info("enter import_line_ch")
    table = name.replace('.txt', '').replace('am_', '').replace('pm_', '')

    def iter_csv(filename):
        print(filename)
        reader = DictReader(f)
        print(reader)

        # for line in reader:

        # yield {k: (converters[k](v) if k in converters else v) for k, v in line.items()}

        client.execute('INSERT INTO %s VALUES', table, iter_csv(name))

    read_count_max = 10
    i = 0
    for a in f:
        print(a)
        # read a line from file
        a = f.readline()
        # logging.debug(a)
        a = a[0:-1]
        # logging.debug(a)
        # split the line into tokens
        # token = re.split(r'\t+',a)
        token = re.split('\t+', a)
        # logging.debug(token)

        if table == 'hq_index':
            # logging.debug ('need one less element')
            token.remove('')

        # sql = construct_insert_statement_ch(table, token)
        sql = 'insert into ' + table + ' values ('
        for b in token:
            sql += b + ', '
        sql = sql[:-2] + ')'

        logging.debug(sql)
        client.execute(sql)

        if i == read_count_max:
            # mydb.commit()
            break
        else:
            i += 1

    logging.debug("continue")

    #    for x in f:
    #        logging.debug(x)
    #
    #
    #        # read a line from file
    #        a = f.readline()
    #        logging.debug(a)
    #        # split the line into tokens
    #        token = re.split(r'\t+',a)
    #        logging.debug(token)

    #
    #    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    #    val = ("John", "Highway 21")
    #
    #    sql = "Show Tables"
    #    mycursor.execute(sql)
    #
    #
    #    #mydb.commit()
    #
    #    #logging.debug(mycursor.rowcount, "record inserted.")
    #    #logging.debug("1 record inserted, ID:", mycursor.lastrowid)
    #
    #    #mycursor.execute("SELECT * FROM customers")
    #
    #    #myresult = mycursor.fetchall()
    #
    #    for x in mycursor:
    #      logging.debug(x)


def construct_insert_statement(table, token):
    mycursor = mydb.cursor()

    # logging.debug(name)
    # table = name.replace('.txt','').replace('am_','').replace('pm_','')

    # logging.debug(table)

    sql = """
    select column_name
    from information_schema.columns
    where table_schema = 'shenzhendata'
    """
    sql = sql + ' and table_name = \'' + table + '\'\n'
    sql = sql + 'ORDER BY ORDINAL_POSITION'

    # logging.debug(sql)
    mycursor.execute(sql)

    sql2 = 'insert into ' + table + ' ('
    sql3 = ' values ('

    for x in mycursor:
        sql2 = sql2 + x[0] + ','
        # sql3 = sql3 + '%s,'
        # if not mycursor.last
        # logging.debug(x)
        # logging.debug(sql2)
        # logging.debug(sql3)

    for i in token:
        if i == 'NULL':
            sql3 = sql3 + 'null,'
        else:
            sql3 = sql3 + '\'' + str(i) + '\','

    sql2 = sql2[:-1] + ')'
    sql3 = sql3[:-1] + ')'
    # logging.debug(sql2)
    # logging.debug(sql3)

    sql = sql2 + sql3
    # logging.debug(sql)

    return sql


def construct_insert_statement_ch(table, token):
    logging.info('construct_insert_statement_ch')
    # table = name.replace('.txt','').replace('am_','').replace('pm_','')

    # logging.debug(table)

    sql = """
    select column_name
    from information_schema.columns
    where table_schema = 'shenzhendata'
    """
    sql = sql + ' and table_name = \'' + table + '\'\n'
    sql = sql + 'ORDER BY ORDINAL_POSITION'

    logging.debug(sql)
    column_name = client.execute(sql)

    sql2 = 'insert into ' + table + ' ('
    sql3 = ' values ('

    for x in column_name:
        sql2 = sql2 + x[0] + ','
        # sql3 = sql3 + '%s,'
        # if not mycursor.last
        # logging.debug(x)
        # logging.debug(sql2)
        # logging.debug(sql3)

    for i in token:
        if i == 'NULL':
            sql3 = sql3 + 'null,'
        else:
            sql3 = sql3 + '\'' + str(i) + '\','

    sql2 = sql2[:-1] + ')'
    sql3 = sql3[:-1] + ')'
    # logging.debug(sql2)
    # logging.debug(sql3)

    sql = sql2 + sql3
    # logging.debug(sql)

    return sql


def import_to_db(opt):
    # prerequisite need to have data file in the 'raw' directory
    # specify where the data file located
    # opt 1 mysql 2 clickhouse

    raw_path = "../../../PythonProj/raw"
    # prepro_path = "./prepro"

    # Walking a directory tree and printing the names of the directories and files
    for (dir_path, dir_names, files) in os.walk(raw_path):
        logging.debug(f'Found directory: {dir_path}')
        for file_name in files:
            logging.debug(file_name)
            f = open(dir_path + "/" + file_name, "r")
            # for x in f:
            #     logging.debug(x)
            if opt == '1':
                import_line(f, file_name)
            elif opt == '2':
                import_line_ch(f, file_name)
            logging.debug('importing data')

            f.close()


def prepro_init(raw_path, prepro_path):
    # delete items in the destination path
    print(raw_path)
    folder = prepro_path
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            logging.debug('Failed to delete %s. Reason: %s' % (file_path, e))


def prepro_copy(raw_path, prepro_path):
    # copy the content from raw to preprocess folder
    src = raw_path
    dst = prepro_path
    symlinks = False
    ignore = None

    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


def prepro_concat(prepro_path):
    # make file shorter for the purpose of testing

    # Walking a directory tree and printing the names of the directories and files
    for dir_path, dir_names, files in os.walk(prepro_path):
        logging.debug(f'Found directory: {dir_path}')
        for file_name in files:
            logging.debug(file_name)
            f = open(dir_path + "/" + file_name, "r")

            # test2(f,file_name,dir_path,dir_names,prepro_path)

            f.close()

    return


def preprocessing():
    raw_path = "D:/StockProjectData/raw"
    prepro_path = "D:/StockProjectData/prepro"
    tar_path = "D:/StockProjectData/tar"

    def reset():
        # clear all prepro and raw files and make new folder
        try:
            shutil.rmtree(prepro_path)
        except Exception as e1:
            print(e1)
        try:
            os.mkdir(prepro_path)
        except Exception as e1:
            print(e1)
        try:
            shutil.rmtree(raw_path)
        except Exception as e1:
            print(e1)
        try:
            os.mkdir(raw_path)
        except Exception as e1:
            print(e1)

    reset()

    def extract():
        # extract tar files
        for (dir_path1, dir_names, files1) in os.walk(tar_path):
            file_list = set([])
            target_path1 = dir_path1.replace('tar', 'raw')
            try:
                os.mkdir(target_path1)
            except Exception as e1:
                print(e1)
            # get list of file
            for file_name1 in files1:
                print(file_name1)
                file_list.add(file_name1[:-4])
                print(file_list)
            # extraction
            for file in file_list:
                print(dir_path1 + '/' + file)
                with multivolumefile.open(dir_path1 + '/' + file, mode='rb') as target_archive:
                    with SevenZipFile(target_archive, 'r') as archive:
                        archive.extractall(target_path1)

    extract()

    def process():
        # process raw files
        for (dir_path, dir_names, files) in os.walk(raw_path):
            target_path = dir_path.replace('raw', 'prepro')
            try:
                os.mkdir(target_path)
            except Exception as e:
                print(e)
            for file_name in files:
                f = open(dir_path + "/" + file_name, "r")
                w = open(target_path + "/" + file_name, "w")
                print(dir_path, dir_names, file_name)
                limit = 10
                next_n_lines = list(islice(f, limit))
                # print(next_n_lines)
                w.writelines(next_n_lines)
                f.close()
                w.close()

    process()


def reset_db():
    # option 1 MySQL
    # option 2 Clickhouse
    logging.info('Calling resetDB')
    global mydb
    global client
    # option = 1

    if option == 1:
        mydb = mysql.connector.connect(
            # connect to db
            host="127.0.0.1",
            user="admin",
            passwd="password",
            auth_plugin='mysql_native_password'
        )
        mycursor = mydb.cursor()
        # drop database
        try:
            mycursor.execute("DROP DATABASE shenzhendata")
        except Exception as e:
            print(e)
            logging.warning('mysql drop database unsuccessful')
        mycursor.execute("CREATE DATABASE shenzhendata")
        mydb = mysql.connector.connect(
            # connect to db
            host="127.0.0.1",
            user="admin",
            passwd="password",
            database="shenzhendata"
        )
        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")
        for x in mycursor:
            logging.debug(x)
        init_table(mydb)
    elif option == 2:
        try:
            client.execute("DROP DATABASE shenzhendata")
        except Exception as e:
            print(e)
            logging.warning('clickhouse drop database error')
        client.execute("CREATE DATABASE shenzhendata")
        init_table_ch()
    else:
        logging.debug("invalid option")


def reset_db_ch():
    logging.info('Calling reset_db_ch')
    global client

    try:
        client.execute("DROP DATABASE shenzhendata")
    except Exception as e:
        print(e)
        logging.warning('clickhouse drop database error')
    client.execute("CREATE DATABASE shenzhendata")
    init_table_ch()
    logging.debug(client.execute('show tables in shenzhendata'))


def db_connect():
    # connect to database
    # option 1: MySQL

    logging.info('Calling db_connect')
    global mydb

    try:
        mydb = mysql.connector.connect(
            # connect to db
            host="127.0.0.1",
            user="admin",
            passwd="password",
            database="shenzhendata",
            auth_plugin='mysql_native_password'
        )
        # logging.debug(mydb)
        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")
        logging.debug('show all tables')
        for x in mycursor:
            logging.debug(x)
    except Exception as e:
        print(e)
        # shenzhentable not setup, create shenzhen table
        reset_db()
        #
        # mydb = mysql.connector.connect(
        #     #connect to db
        #   host="127.0.0.1",
        #   user="admin",
        #   passwd="password"
        # )
        # mycursor = mydb.cursor()
        #
        #
        # # init table
        # mycursor.execute("CREATE DATABASE shenzhendata")
        # client.execute('CREATE DATABASE shenzhendata')
        #
        # mydb = mysql.connector.connect(
        #     #connect to db
        #   host="127.0.0.1",
        #   user="admin",
        #   passwd="password",
        #   database="shenzhendata"
        # )
        #
        # mycursor = mydb.cursor()
        #
        # # init table
        # init_table(mydb, client)


def db_connect_ch():
    # connect to database
    # option 1: MySQL
    # option 2: ClickHouse

    logging.info('Calling db_connect_ch')
    global client

    # resetDB()
    try:
        # try connect with shenzhendata table
        client = Client(host='localhost', password='ABCDE', database='shenzhendata')
        logging.debug(client.execute('SHOW DATABASES'))
    except Exception as e:
        print(e)
        reset_db_ch()


def init_table(db):
    logging.info('Calling init_table')

    mycursor = db.cursor()

    # stock_status table
    sql = """
    CREATE TABLE stock_status (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    FinancialStatus CHAR(8),
    FinancialLongFlag CHAR,
    LendSecurityShortFlag CHAR,
    SubscribeFlag CHAR,
    RedeemFlag CHAR,
    OfferBuyFlag CHAR,
    ConvertStockFlag CHAR,
    PutBackFlag CHAR,
    ExerciseFlag CHAR,
    LongOpenFlag CHAR,
    ShortOpenFlag CHAR,
    GoldETFMaterialOfferbuyFlag CHAR,
    GoldETFMaterialRedeemFlag CHAR,
    ADReciveOfferFlag CHAR,
    RemoveOfferFlag CHAR,
    ConverStockCancelFlag CHAR,
    PutbackCancelFlag CHAR,
    PledgeFlag CHAR,
    UnresolvedChangeFlag CHAR,
    Vote CHAR,
    StockPledgedRepurchase CHAR,
    RealtimeSplit CHAR,
    RealtimeCombine CHAR,
    CoveredCall CHAR,
    MarketMakerOfferPrice CHAR)
    """
    mycursor.execute(sql)

    # hq_snap_spot
    sql = """
    CREATE TABLE hq_snap_spot (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    PreClosePx DEC(9,3),
    PxChange1 DEC(9,3),
    PxChange2 DEC(9,3),
    OpenPx DEC(9,3),
    HighPx DEC(9,3),
    LowPx DEC(9,3),
    LastPx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumnTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    PERatio1 DEC(7,2),
    PERatio2 DEC(7,2),
    TradingPhaseCode CHAR(2),
    totalofferqty DEC(12,0),
    weightedavgofferpx DEC(9,3),
    totalbidqty DEC(12,0),
    weightedavgbidpx DEC(9,3),
    TBD1 DEC(12,0),
    TBD2 DEC(12,0),
    TBD3 DEC(12,0),
    PreNAV DEC(16,6),
    RealTimeNAV DEC(12,6),
    WarrantPremiumRate DEC(9,3),
    UpLimitPx DEC(9,3),
    DownLimitPx DEC(9,3),
    TotalLongPosition DEC(12,0)    
    )
    """
    mycursor.execute(sql)

    # hq_snap_option
    sql = """
    CREATE TABLE hq_snap_option (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    PreClosePx DEC(9,3),
    PxChange1 DEC(9,3),
    PxChange2 DEC(9,3),
    OpenPx DEC(9,3),
    HighPx DEC(9,3),
    LowPx DEC(9,3),
    LastPx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumnTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    PERatio1 DEC(7,2),
    PERatio2 DEC(7,2),
    TradingPhaseCode CHAR(2),
    totalofferqty DEC(12,0),
    weightedavgofferpx DEC(9,3),
    totalbidqty DEC(12,0),
    weightedavgbidpx DEC(9,3),
    PreNAV DEC(12,6),
    RealTimeNAV DEC(12,6),
    WarrantPremiumRate DEC(9,3),
    UpLimitPx DEC(9,3),
    DownLimitPx DEC(9,3),
    TotalLongPosition DEC(12,0))
    """
    mycursor.execute(sql)

    # hq_snap_pledge
    sql = """
    CREATE TABLE hq_snap_pledge (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    PreClosePx DEC(9,3),
    PxChange1 DEC(9,3),
    PxChange2 DEC(9,3),
    OpenPx DEC(9,3),
    HighPx DEC(9,3),
    LowPx DEC(9,3),
    LastPx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumnTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    PERatio1 DEC(7,2),
    PERatio2 DEC(7,2),
    TradingPhaseCode CHAR(2),
    totalofferqty DEC(12,0),
    weightedavgofferpx DEC(9,3),
    totalbidqty DEC(12,0),
    weightedavgbidpx DEC(9,3),
    TBD1 DEC(12,0),
    TBD2 DEC(12,0),
    TBD3 DEC(12,0),
    PreNAV DEC(16,6),
    RealTimeNAV DEC(12,6),
    WarrantPremiumRate DEC(9,3),
    UpLimitPx DEC(9,3),
    DownLimitPx DEC(9,3),
    TotalLongPosition DEC(12,0)
    )
    """

    mycursor.execute(sql)

    # hq_snap_bond
    sql = """
    CREATE TABLE hq_snap_bond (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    PreClosePx DEC(9,3),
    PxChange1 DEC(9,3),
    PxChange2 DEC(9,3),
    OpenPx DEC(9,3),
    HighPx DEC(9,3),
    LowPx DEC(9,3),
    LastPx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumnTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    PERatio1 DEC(7,2),
    PERatio2 DEC(7,2),
    TradingPhaseCode CHAR(2),
    totalofferqty DEC(12,0),
    weightedavgofferpx DEC(9,3),
    totalbidqty DEC(12,0),
    weightedavgbidpx DEC(9,3),
    PreNAV DEC(12,6),
    RealTimeNAV DEC(12,6),
    WarrantPremiumRate DEC(9,3),
    UpLimitPx DEC(9,3),
    DownLimitPx DEC(9,3),
    TotalLongPosition DEC(12,0))
    """

    mycursor.execute(sql)

    # hq_closeSnape
    sql = """
    CREATE TABLE hq_closeSnape (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    PreClosePx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumeTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    TradingPhaseCode CHAR(2),
    buypx DEC(9,3),
    buynum DEC(12,0),
    sellpx DEC(9,3),
    sellnum DEC(12,0)
    )
    """

    mycursor.execute(sql)

    # snap_level_spot
    sql = """
    CREATE TABLE snap_level_spot (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    OfferPX1 DEC(9,3),
    BidPX1 DEC(9,3),
    OfferSize1 DEC(12),
    BidSize1 DEC(12),
    OfferPX2 DEC(9,3),
    BidPX2 DEC(9,3),
    OfferSize2 DEC(12),
    BidSize2 DEC(12),
    OfferPX3 DEC(9,3),
    BidPX3 DEC(9,3),
    OfferSize3 DEC(12),
    BidSize3 DEC(12),
    OfferPX4 DEC(9,3),
    BidPX4 DEC(9,3),
    OfferSize4 DEC(12),
    BidSize4 DEC(12),
    OfferPX5 DEC(9,3),
    BidPX5 DEC(9,3),
    OfferSize5 DEC(12),
    BidSize5 DEC(12),
    OfferPX6 DEC(9,3),
    BidPX6 DEC(9,3),
    OfferSize6 DEC(12),
    BidSize6 DEC(12),
    OfferPX7 DEC(9,3),
    BidPX7 DEC(9,3),
    OfferSize7 DEC(12),
    BidSize7 DEC(12),
    OfferPX8 DEC(9,3),
    BidPX8 DEC(9,3),
    OfferSize8 DEC(12),
    BidSize8 DEC(12),
    OfferPX9 DEC(9,3),
    BidPX9 DEC(9,3),
    OfferSize9 DEC(12),
    BidSize9 DEC(12),
    OfferPX10 DEC(9,3),
    BidPX10 DEC(9,3),
    OfferSize10 DEC(12),
    BidSize10 DEC(12),
    NUMORDERS_B1 DEC(10),
    NOORDERS_B1 DEC(10),
    ORDERQTY_B1 VARCHAR(512),
    NUMORDERS_S1 DEC(10),
    NOORDERS_S1 DEC(10),
    ORDERQTY_S1 VARCHAR(512)        
    )
    """

    mycursor.execute(sql)

    # snap_level_option
    sql = """
    CREATE TABLE snap_level_option (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    OfferPX1 DEC(9,3),
    BidPX1 DEC(9,3),
    OfferSize1 DEC(12),
    BidSize1 DEC(12),
    OfferPX2 DEC(9,3),
    BidPX2 DEC(9,3),
    OfferSize2 DEC(12),
    BidSize2 DEC(12),
    OfferPX3 DEC(9,3),
    BidPX3 DEC(9,3),
    OfferSize3 DEC(12),
    BidSize3 DEC(12),
    OfferPX4 DEC(9,3),
    BidPX4 DEC(9,3),
    OfferSize4 DEC(12),
    BidSize4 DEC(12),
    OfferPX5 DEC(9,3),
    BidPX5 DEC(9,3),
    OfferSize5 DEC(12),
    BidSize5 DEC(12),
    OfferPX6 DEC(9,3),
    BidPX6 DEC(9,3),
    OfferSize6 DEC(12),
    BidSize6 DEC(12),
    OfferPX7 DEC(9,3),
    BidPX7 DEC(9,3),
    OfferSize7 DEC(12),
    BidSize7 DEC(12),
    OfferPX8 DEC(9,3),
    BidPX8 DEC(9,3),
    OfferSize8 DEC(12),
    BidSize8 DEC(12),
    OfferPX9 DEC(9,3),
    BidPX9 DEC(9,3),
    OfferSize9 DEC(12),
    BidSize9 DEC(12),
    OfferPX10 DEC(9,3),
    BidPX10 DEC(9,3),
    OfferSize10 DEC(12),
    BidSize10 DEC(12),
    NUMORDERS_B1 DEC(10),
    NOORDERS_B1 DEC(10),
    ORDERQTY_B1 VARCHAR(512),
    NUMORDERS_S1 DEC(10),
    NOORDERS_S1 DEC(10),
    ORDERQTY_S1 VARCHAR(512)        
    )
    """

    mycursor.execute(sql)

    # snap_level_pledge
    sql = """
    CREATE TABLE snap_level_pledge (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    OfferPX1 DEC(9,3),
    BidPX1 DEC(9,3),
    OfferSize1 DEC(12),
    BidSize1 DEC(12),
    OfferPX2 DEC(9,3),
    BidPX2 DEC(9,3),
    OfferSize2 DEC(12),
    BidSize2 DEC(12),
    OfferPX3 DEC(9,3),
    BidPX3 DEC(9,3),
    OfferSize3 DEC(12),
    BidSize3 DEC(12),
    OfferPX4 DEC(9,3),
    BidPX4 DEC(9,3),
    OfferSize4 DEC(12),
    BidSize4 DEC(12),
    OfferPX5 DEC(9,3),
    BidPX5 DEC(9,3),
    OfferSize5 DEC(12),
    BidSize5 DEC(12),
    OfferPX6 DEC(9,3),
    BidPX6 DEC(9,3),
    OfferSize6 DEC(12),
    BidSize6 DEC(12),
    OfferPX7 DEC(9,3),
    BidPX7 DEC(9,3),
    OfferSize7 DEC(12),
    BidSize7 DEC(12),
    OfferPX8 DEC(9,3),
    BidPX8 DEC(9,3),
    OfferSize8 DEC(12),
    BidSize8 DEC(12),
    OfferPX9 DEC(9,3),
    BidPX9 DEC(9,3),
    OfferSize9 DEC(12),
    BidSize9 DEC(12),
    OfferPX10 DEC(9,3),
    BidPX10 DEC(9,3),
    OfferSize10 DEC(12),
    BidSize10 DEC(12),
    NUMORDERS_B1 DEC(10),
    NOORDERS_B1 DEC(10),
    ORDERQTY_B1 VARCHAR(512),
    NUMORDERS_S1 DEC(10),
    NOORDERS_S1 DEC(10),
    ORDERQTY_S1 VARCHAR(512)        
    )
    """

    mycursor.execute(sql)

    # snap_level_bond
    sql = """
    CREATE TABLE snap_level_bond (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    MDStreamID CHAR(3),
    OfferPX1 DEC(9,3),
    BidPX1 DEC(9,3),
    OfferSize1 DEC(12),
    BidSize1 DEC(12),
    OfferPX2 DEC(9,3),
    BidPX2 DEC(9,3),
    OfferSize2 DEC(12),
    BidSize2 DEC(12),
    OfferPX3 DEC(9,3),
    BidPX3 DEC(9,3),
    OfferSize3 DEC(12),
    BidSize3 DEC(12),
    OfferPX4 DEC(9,3),
    BidPX4 DEC(9,3),
    OfferSize4 DEC(12),
    BidSize4 DEC(12),
    OfferPX5 DEC(9,3),
    BidPX5 DEC(9,3),
    OfferSize5 DEC(12),
    BidSize5 DEC(12),
    OfferPX6 DEC(9,3),
    BidPX6 DEC(9,3),
    OfferSize6 DEC(12),
    BidSize6 DEC(12),
    OfferPX7 DEC(9,3),
    BidPX7 DEC(9,3),
    OfferSize7 DEC(12),
    BidSize7 DEC(12),
    OfferPX8 DEC(9,3),
    BidPX8 DEC(9,3),
    OfferSize8 DEC(12),
    BidSize8 DEC(12),
    OfferPX9 DEC(9,3),
    BidPX9 DEC(9,3),
    OfferSize9 DEC(12),
    BidSize9 DEC(12),
    OfferPX10 DEC(9,3),
    BidPX10 DEC(9,3),
    OfferSize10 DEC(12),
    BidSize10 DEC(12),
    NUMORDERS_B1 DEC(10),
    NOORDERS_B1 DEC(10),
    ORDERQTY_B1 VARCHAR(512),
    NUMORDERS_S1 DEC(10),
    NOORDERS_S1 DEC(10),
    ORDERQTY_S1 VARCHAR(512)        
    )
    """

    mycursor.execute(sql)

    # hq_index
    sql = """
    CREATE TABLE hq_index (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    PreClosePx DEC(9,3),
    OpenPx DEC(9,3),
    HighPx DEC(9,3),
    LowPx DEC(9,3),
    LastPx DEC(9,3),
    NumTrades DEC(9,0),
    TotalVolumnTrade DEC(12,0),
    TotalValueTrade DEC(17,3),
    TradingPhaseCode CHAR(2)    
    )
    """

    mycursor.execute(sql)

    # hq_order_spot
    sql = """
    CREATE TABLE hq_order_spot (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    Price DEC(9,3),
    OrderQty DEC(9,0),
    TransactTime BIGINT,
    Side CHAR(2),
    OrderType CHAR(2),
    ConfirmID DEC(9,3),
    Contractor CHAR(20),
    ContactInfo CHAR(50),
    ExpirationDays DEC(8,0),
    ExpirationType DEC(8,0)
    )
    """

    mycursor.execute(sql)

    # hq_order_agreement
    sql = """
    CREATE TABLE hq_order_agreement (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    Price DEC(9,3),
    OrderQty DEC(9,0),
    TransactTime BIGINT,
    Side CHAR(2),
    OrderType CHAR(2),
    ConfirmID DEC(9,3),
    Contractor CHAR(20),
    ContactInfo CHAR(50),
    ExpirationDays DEC(8,0),
    ExpirationType DEC(8,0)
    )
    """

    mycursor.execute(sql)

    # hq_order_refinance
    sql = """
    CREATE TABLE hq_order_refinance (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    Price DEC(9,3),
    OrderQty DEC(9,0),
    TransactTime BIGINT,
    Side CHAR(2),
    OrderType CHAR(2),
    ConfirmID DEC(9,3),
    Contractor CHAR(20),
    ContactInfo CHAR(50),
    ExpirationDays DEC(8,0),
    ExpirationType DEC(8,0)
    )
    """

    mycursor.execute(sql)

    # hq_order_pledge
    sql = """
    CREATE TABLE hq_order_pledge (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    Price DEC(9,3),
    OrderQty DEC(9,0),
    TransactTime BIGINT,
    Side CHAR(2),
    OrderType CHAR(2),
    ConfirmID DEC(9,3),
    Contractor CHAR(20),
    ContactInfo CHAR(50),
    ExpirationDays DEC(8,0),
    ExpirationType DEC(8,0)
    )
    """

    mycursor.execute(sql)

    # hq_order_option
    sql = """
    CREATE TABLE hq_order_option (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    Price DEC(9,3),
    OrderQty DEC(9,0),
    TransactTime BIGINT,
    Side CHAR(2),
    OrderType CHAR(2),
    ConfirmID DEC(9,3),
    Contractor CHAR(20),
    ContactInfo CHAR(50),
    ExpirationDays DEC(8,0),
    ExpirationType DEC(8,0)
    )
    """

    mycursor.execute(sql)

    # hq_trade_spot
    sql = """
    CREATE TABLE hq_trade_spot (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    BidApplSeqNum BIGINT,
    OfferApplSeqNum BIGINT,
    Price DEC(9,3),
    TradeQty DEC(9,0),
    ExecType CHAR(2),
    TradeTime BIGINT        
    )
    """

    mycursor.execute(sql)

    # hq_trade_agreement
    sql = """
    CREATE TABLE hq_trade_agreement (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    BidApplSeqNum BIGINT,
    OfferApplSeqNum BIGINT,
    Price DEC(9,3),
    TradeQty DEC(9,0),
    ExecType CHAR(2),
    TradeTime BIGINT        
    )
    """

    mycursor.execute(sql)

    # hq_trade_refinance
    sql = """
    CREATE TABLE hq_trade_refinance (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    BidApplSeqNum BIGINT,
    OfferApplSeqNum BIGINT,
    Price DEC(9,3),
    TradeQty DEC(9,0),
    ExecType CHAR(2),
    TradeTime BIGINT        
    )
    """

    mycursor.execute(sql)

    # hq_trade_pledge
    sql = """
    CREATE TABLE hq_trade_pledge (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    BidApplSeqNum BIGINT,
    OfferApplSeqNum BIGINT,
    Price DEC(9,3),
    TradeQty DEC(9,0),
    ExecType CHAR(2),
    TradeTime BIGINT        
    )
    """

    mycursor.execute(sql)

    # hq_trade_option
    sql = """
    CREATE TABLE hq_trade_option (
    TradeDate DEC(8,0),
    OrigTime BIGINT,
    SendTime BIGINT,
    Recvtime BIGINT,
    dbtime BIGINT,
    ChannelNo INT(16) UNSIGNED,
    MDStreamID CHAR(3),
    ApplSeqNum BIGINT,
    SecurityID CHAR(8),
    SecurityIDSource CHAR(4),
    BidApplSeqNum BIGINT,
    OfferApplSeqNum BIGINT,
    Price DEC(9,3),
    TradeQty DEC(9,0),
    ExecType CHAR(2),
    TradeTime BIGINT        
    )
    """

    mycursor.execute(sql)


def init_table_ch():
    logging.info('Calling init_table_ch')

    # logging.debug('add stock_status')
    # stock_status table
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.stock_status (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    FinancialStatus String,
    FinancialLongFlag String,
    LendSecurityShortFlag String,
    SubscribeFlag String,
    RedeemFlag String,
    OfferBuyFlag String,
    ConvertStockFlag String,
    PutBackFlag String,
    ExerciseFlag String,
    LongOpenFlag String,
    ShortOpenFlag String,
    GoldETFMaterialOfferbuyFlag String,
    GoldETFMaterialRedeemFlag String,
    ADReciveOfferFlag String,
    RemoveOfferFlag String,
    ConverStockCancelFlag String,
    PutbackCancelFlag String,
    PledgeFlag String,
    UnresolvedChangeFlag String,
    Vote String,
    StockPledgedRepurchase String,
    RealtimeSplit String,
    RealtimeCombine String,
    CoveredCall String,
    MarketMakerOfferPrice String) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_spot')
    # hq_snap_spot
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_snap_spot (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    PreClosePx Double,
    PxChange1 Double,
    PxChange2 Double,
    OpenPx Double,
    HighPx Double,
    LowPx Double,
    LastPx Double,
    NumTrades Double,
    TotalVolumnTrade Double,
    TotalValueTrade Double,
    PERatio1 Double,
    PERatio2 Double,
    TradingPhaseCode String,
    totalofferqty Double,
    weightedavgofferpx Double,
    totalbidqty Double,
    weightedavgbidpx Double,
    TBD1 Double,
    TBD2 Double,
    TBD3 Double,
    PreNAV Double,
    RealTimeNAV Double,
    WarrantPremiumRate Double,
    UpLimitPx Double,
    DownLimitPx Double,
    TotalLongPosition Double    
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_snap_option
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_snap_option (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    PreClosePx Double,
    PxChange1 Double,
    PxChange2 Double,
    OpenPx Double,
    HighPx Double,
    LowPx Double,
    LastPx Double,
    NumTrades Double,
    TotalVolumnTrade Double,
    TotalValueTrade Double,
    PERatio1 Double,
    PERatio2 Double,
    TradingPhaseCode String,
    totalofferqty Double,
    weightedavgofferpx Double,
    totalbidqty Double,
    weightedavgbidpx Double,
    PreNAV Double,
    RealTimeNAV Double,
    WarrantPremiumRate Double,
    UpLimitPx Double,
    DownLimitPx Double,
    TotalLongPosition Double) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_snap_pledge
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_snap_pledge (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    PreClosePx Double,
    PxChange1 Double,
    PxChange2 Double,
    OpenPx Double,
    HighPx Double,
    LowPx Double,
    LastPx Double,
    NumTrades Double,
    TotalVolumnTrade Double,
    TotalValueTrade Double,
    PERatio1 Double,
    PERatio2 Double,
    TradingPhaseCode String,
    totalofferqty Double,
    weightedavgofferpx Double,
    totalbidqty Double,
    weightedavgbidpx Double,
    TBD1 Double,
    TBD2 Double,
    TBD3 Double,
    PreNAV Double,
    RealTimeNAV Double,
    WarrantPremiumRate Double,
    UpLimitPx Double,
    DownLimitPx Double,
    TotalLongPosition Double
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_snap_bond
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_snap_bond (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    PreClosePx Double,
    PxChange1 Double,
    PxChange2 Double,
    OpenPx Double,
    HighPx Double,
    LowPx Double,
    LastPx Double,
    NumTrades Double,
    TotalVolumnTrade Double,
    TotalValueTrade Double,
    PERatio1 Double,
    PERatio2 Double,
    TradingPhaseCode String,
    totalofferqty Double,
    weightedavgofferpx Double,
    totalbidqty Double,
    weightedavgbidpx Double,
    PreNAV Double,
    RealTimeNAV Double,
    WarrantPremiumRate Double,
    UpLimitPx Double,
    DownLimitPx Double,
    TotalLongPosition Double) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_closeSnape
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_closeSnape (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    PreClosePx Double,
    NumTrades Double,
    TotalVolumeTrade Double,
    TotalValueTrade Double,
    TradingPhaseCode String,
    buypx Double,
    buynum Double,
    sellpx Double,
    sellnum Double
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # snap_level_spot
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.snap_level_spot (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    OfferPX1 Double,
    BidPX1 Double,
    OfferSize1 Double,
    BidSize1 Double,
    OfferPX2 Double,
    BidPX2 Double,
    OfferSize2 Double,
    BidSize2 Double,
    OfferPX3 Double,
    BidPX3 Double,
    OfferSize3 Double,
    BidSize3 Double,
    OfferPX4 Double,
    BidPX4 Double,
    OfferSize4 Double,
    BidSize4 Double,
    OfferPX5 Double,
    BidPX5 Double,
    OfferSize5 Double,
    BidSize5 Double,
    OfferPX6 Double,
    BidPX6 Double,
    OfferSize6 Double,
    BidSize6 Double,
    OfferPX7 Double,
    BidPX7 Double,
    OfferSize7 Double,
    BidSize7 Double,
    OfferPX8 Double,
    BidPX8 Double,
    OfferSize8 Double,
    BidSize8 Double,
    OfferPX9 Double,
    BidPX9 Double,
    OfferSize9 Double,
    BidSize9 Double,
    OfferPX10 Double,
    BidPX10 Double,
    OfferSize10 Double,
    BidSize10 Double,
    NUMORDERS_B1 Double,
    NOORDERS_B1 Double,
    ORDERQTY_B1 String,
    NUMORDERS_S1 Double,
    NOORDERS_S1 Double,
    ORDERQTY_S1 String        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # snap_level_option
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.snap_level_option (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    OfferPX1 Double,
    BidPX1 Double,
    OfferSize1 Double,
    BidSize1 Double,
    OfferPX2 Double,
    BidPX2 Double,
    OfferSize2 Double,
    BidSize2 Double,
    OfferPX3 Double,
    BidPX3 Double,
    OfferSize3 Double,
    BidSize3 Double,
    OfferPX4 Double,
    BidPX4 Double,
    OfferSize4 Double,
    BidSize4 Double,
    OfferPX5 Double,
    BidPX5 Double,
    OfferSize5 Double,
    BidSize5 Double,
    OfferPX6 Double,
    BidPX6 Double,
    OfferSize6 Double,
    BidSize6 Double,
    OfferPX7 Double,
    BidPX7 Double,
    OfferSize7 Double,
    BidSize7 Double,
    OfferPX8 Double,
    BidPX8 Double,
    OfferSize8 Double,
    BidSize8 Double,
    OfferPX9 Double,
    BidPX9 Double,
    OfferSize9 Double,
    BidSize9 Double,
    OfferPX10 Double,
    BidPX10 Double,
    OfferSize10 Double,
    BidSize10 Double,
    NUMORDERS_B1 Double,
    NOORDERS_B1 Double,
    ORDERQTY_B1 String,
    NUMORDERS_S1 Double,
    NOORDERS_S1 Double,
    ORDERQTY_S1 String        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # snap_level_pledge
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.snap_level_pledge (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    OfferPX1 Double,
    BidPX1 Double,
    OfferSize1 Double,
    BidSize1 Double,
    OfferPX2 Double,
    BidPX2 Double,
    OfferSize2 Double,
    BidSize2 Double,
    OfferPX3 Double,
    BidPX3 Double,
    OfferSize3 Double,
    BidSize3 Double,
    OfferPX4 Double,
    BidPX4 Double,
    OfferSize4 Double,
    BidSize4 Double,
    OfferPX5 Double,
    BidPX5 Double,
    OfferSize5 Double,
    BidSize5 Double,
    OfferPX6 Double,
    BidPX6 Double,
    OfferSize6 Double,
    BidSize6 Double,
    OfferPX7 Double,
    BidPX7 Double,
    OfferSize7 Double,
    BidSize7 Double,
    OfferPX8 Double,
    BidPX8 Double,
    OfferSize8 Double,
    BidSize8 Double,
    OfferPX9 Double,
    BidPX9 Double,
    OfferSize9 Double,
    BidSize9 Double,
    OfferPX10 Double,
    BidPX10 Double,
    OfferSize10 Double,
    BidSize10 Double,
    NUMORDERS_B1 Double,
    NOORDERS_B1 Double,
    ORDERQTY_B1 String,
    NUMORDERS_S1 Double,
    NOORDERS_S1 Double,
    ORDERQTY_S1 String        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # snap_level_bond
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.snap_level_bond (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    SecurityID String,
    SecurityIDSource String,
    MDStreamID String,
    OfferPX1 Double,
    BidPX1 Double,
    OfferSize1 Double,
    BidSize1 Double,
    OfferPX2 Double,
    BidPX2 Double,
    OfferSize2 Double,
    BidSize2 Double,
    OfferPX3 Double,
    BidPX3 Double,
    OfferSize3 Double,
    BidSize3 Double,
    OfferPX4 Double,
    BidPX4 Double,
    OfferSize4 Double,
    BidSize4 Double,
    OfferPX5 Double,
    BidPX5 Double,
    OfferSize5 Double,
    BidSize5 Double,
    OfferPX6 Double,
    BidPX6 Double,
    OfferSize6 Double,
    BidSize6 Double,
    OfferPX7 Double,
    BidPX7 Double,
    OfferSize7 Double,
    BidSize7 Double,
    OfferPX8 Double,
    BidPX8 Double,
    OfferSize8 Double,
    BidSize8 Double,
    OfferPX9 Double,
    BidPX9 Double,
    OfferSize9 Double,
    BidSize9 Double,
    OfferPX10 Double,
    BidPX10 Double,
    OfferSize10 Double,
    BidSize10 Double,
    NUMORDERS_B1 Double,
    NOORDERS_B1 Double,
    ORDERQTY_B1 String,
    NUMORDERS_S1 Double,
    NOORDERS_S1 Double,
    ORDERQTY_S1 String        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_index
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_index (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    SecurityID String,
    SecurityIDSource String,
    PreClosePx Double,
    OpenPx Double,
    HighPx Double,
    LowPx Double,
    LastPx Double,
    NumTrades Double,
    TotalVolumnTrade Double,
    TotalValueTrade Double,
    TradingPhaseCode String    
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_order_spot
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_order_spot (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    Price Double,
    OrderQty Double,
    TransactTime UInt32,
    Side String,
    OrderType String,
    ConfirmID Double,
    Contractor String,
    ContactInfo String,
    ExpirationDays UInt8,
    ExpirationType UInt8
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_order_agreement
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_order_agreement (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    Price Double,
    OrderQty Double,
    TransactTime UInt32,
    Side String,
    OrderType String,
    ConfirmID Double,
    Contractor String,
    ContactInfo String,
    ExpirationDays UInt8,
    ExpirationType UInt8
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_order_refinance
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_order_refinance (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    Price Double,
    OrderQty Double,
    TransactTime UInt32,
    Side String,
    OrderType String,
    ConfirmID Double,
    Contractor String,
    ContactInfo String,
    ExpirationDays UInt8,
    ExpirationType UInt8
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_order_pledge
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_order_pledge (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    Price Double,
    OrderQty Double,
    TransactTime UInt32,
    Side String,
    OrderType String,
    ConfirmID Double,
    Contractor String,
    ContactInfo String,
    ExpirationDays UInt8,
    ExpirationType UInt8
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_order_option
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_order_option (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    Price Double,
    OrderQty Double,
    TransactTime UInt32,
    Side String,
    OrderType String,
    ConfirmID Double,
    Contractor String,
    ContactInfo String,
    ExpirationDays UInt8,
    ExpirationType UInt8
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_trade_spot
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_trade_spot (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    BidApplSeqNum UInt32,
    OfferApplSeqNum UInt32,
    Price Double,
    TradeQty Double,
    ExecType String,
    TradeTime UInt32        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_trade_agreement
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_trade_agreement (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    BidApplSeqNum UInt32,
    OfferApplSeqNum UInt32,
    Price Double,
    TradeQty Double,
    ExecType String,
    TradeTime UInt32        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_trade_refinance
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_trade_refinance (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    BidApplSeqNum UInt32,
    OfferApplSeqNum UInt32,
    Price Double,
    TradeQty Double,
    ExecType String,
    TradeTime UInt32        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_trade_pledge
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_trade_pledge (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    BidApplSeqNum UInt32,
    OfferApplSeqNum UInt32,
    Price Double,
    TradeQty Double,
    ExecType String,
    TradeTime UInt32        
    ) Engine = Memory
    """
    client.execute(sql)

    # logging.debug('add hq_snap_option')
    # hq_trade_option
    sql = """
    CREATE TABLE IF NOT EXISTS shenzhendata.hq_trade_option (
    TradeDate UInt8,
    OrigTime UInt32,
    SendTime UInt32,
    Recvtime UInt32,
    dbtime UInt32,
    ChannelNo UInt16,
    MDStreamID String,
    ApplSeqNum UInt32,
    SecurityID String,
    SecurityIDSource String,
    BidApplSeqNum UInt32,
    OfferApplSeqNum UInt32,
    Price Double,
    TradeQty Double,
    ExecType String,
    TradeTime UInt32        
    ) Engine = Memory
    """
    client.execute(sql)


def query_db():
    # query the trade spot database

    # get parameter
    start = input('enter start time\t')
    end = input('enter end time\t')
    stock = input('enter stock list\t')
    column = input('column interested: 1 price, 2, volume ')

    print(start, end, stock, column)

    # dummy variable for test
    start = '20190102091500010'
    end = '20190102140000000'
    stock = ['002899', '300548', '000000']
    column = '12'

    result = get_dataset(start, end, stock, column)
    for a in result:
        logging.debug(a)


def kbar_db():
    # ' do the kbar analysis'
    # get parameter
    start = input('enter start time\t')
    end = input('enter end time\t')
    stock = input('enter stock list\t')
    period = input('time period (s,m,h,d,w,M) ')

    print(start, end, stock, period)

    # dummy variable for test
    start = '2019-01-02 09:30:00'
    end = '2019-01-02 13:35:00'
    stock = ['002899', '300548']
    period = 'h'
    result = kbar(start, end, stock, period)
    for i in result:
        logging.debug(i)


def query_db_ch():
    pass


def kbar_db_ch():
    pass


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    log_filename = '../Logs/logfile.log'
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    logging.info('Calling main')

    preprocessing()

    dbOption = input('what is the DB format? 1. MySQL 2.ClickHouse \t')

    global mydb
    global client

    if dbOption == '1':
        db_connect()
        while True:
            option = input('what do you want to do? 1 import, 2 reset, 3 query, 4 kbar analysis,  5 exit \t')
            if option == '1':
                import_to_db(dbOption)
            elif option == '2':
                reset_db()
            elif option == '3':
                query_db()
            elif option == '4':
                kbar_db()
            else:
                logging.debug('not')
                break
    elif dbOption == '2':
        db_connect_ch()
        while True:
            option = input('what do you want to do? 1 import, 2 reset, 3 query, 4 kbar analysis,  5 exit\t')
            if option == '1':
                import_to_db(dbOption)
            elif option == '2':
                reset_db_ch()
            elif option == '3':
                query_db_ch()
            elif option == '4':
                kbar_db_ch()
            else:
                logging.debug('not')
                break
    else:
        logging.debug("bad db_option")

    # if dbOption ==1:
    #     global mydb
    #     mydb = mysql.connector.connect(
    #         # connect to db
    #         host="127.0.0.1",
    #         user="admin",
    #         passwd="password"
    #     )
    # elif dbOption == 2:
    #     global client
    #     client = Client(host='localhost', password='ABCDE',database = 'shenzhendata')
    # else:
    #     logging.info('invalid option')

    # resetDB()

    # whether to create new file to prepare
    # other way is to limit the read lines into the db
    # preprocessing()
    # currently only take 10 lines into the db for easy testing

    while True:
        option = input('what do you want to do? 1 import, 2 reset, 3 query, 4 kbar analysis,  5 exit\t')
        if option == '1':
            import_to_db(dbOption)
        elif option == '2':
            reset_db()
        elif option == '3':
            query_db()
        elif option == '4':
            kbar_db()
        else:
            logging.debug('not')
            break
