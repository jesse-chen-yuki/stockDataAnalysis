import mysql.connector
from zipfile import ZipFile
from datetime import datetime
from datetime import timedelta  
from dateutil.relativedelta import relativedelta
import os
import shutil
import re


def kbar_open(stock, start):
    global mydb
    mycursor = mydb.cursor()
    
    time_str = start.strftime('%Y%m%d%H%M%S%f')
    time_str = time_str[:-3]
    
    #print(time_str)
    sql = '''
        SELECT price FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock +  '''
     and origtime>= '''+time_str+'''
     order by applseqnum
     limit 1'''
    #print(sql)
    
    mycursor.execute(sql)
    
    try:
        result = mycursor.fetchone()[0]
    except:
        return None
    return result
    
    
    
def kbar_high(stock, start,end):
    global mydb
    mycursor = mydb.cursor()
    
    start_str = start.strftime('%Y%m%d%H%M%S%f')[:-3]
    end_str = end.strftime('%Y%m%d%H%M%S%f')[:-3]
    
    sql = '''
        SELECT max(price) FROM shenzhendata.hq_trade_spot 
     where SecurityID = ''' + stock +  '''
     and origtime >= '''+start_str+'''
     and origtime <= ''' + end_str+'''
     order by applseqnum'''
    #print(sql)    
    
    mycursor.execute(sql)
    return mycursor.fetchone()[0]
        
 
 
def kbar_seg(stock,start,end):
    
    open = kbar_open(stock, start)
    print('open is ')
    print(open)
    high = kbar_high(stock, start,end)
    print(high)
    
    
    # analyze the specified stock in the specified time period
    # returns following
    # start time, stock, open, high, low, close, volumn, cmount, num_trades, vwap
    
    return open
    
    
def test(start,end,stock_list,period):
    
    starttime = datetime.fromisoformat(start)
    endtime = datetime.fromisoformat(end)
    #print(starttime)
    #print(endtime)    
    
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
        print(stock)
        
        q_start = starttime
        q_end = starttime+time_increment
        
        while q_start <= endtime:
            # call query function
            result.append(kbar_seg(stock, q_start, q_end))
            
            q_start += time_increment
            q_end += time_increment
        
        
    return result
        
    
def get_dataset(start, end, stock, column):
    # input
    # start: start time format yyyymmddhhmmssxxx
    # end: start time format yyyymmddhhmmssxxx
    # stock: stock list with id format ssssss
    # column: appoint what column to get
    
    # query the db regarding specific stuff
    # return_value = database.get_dataset(start_time, end_time, stock_list, fields = ["price", "volume"]), 
    #return a dataset contains time, stock code, price, volume
    
    global mydb
    mycursor = mydb.cursor()
    
    #print('under test')
    #print(start, end, stock)
    
    sql = 'SELECT origtime, securityID '
    for a in column:
        if a == '1':
            sql = sql + ', price '
        elif a == '2':
            sql = sql + ', tradeqty '
    
    sql = sql +  'FROM shenzhendata.hq_trade_spot \n'
    sql = sql+ ' where origtime >=  \''+start + '\'\n'
    sql = sql+ ' and origtime <=  \''+end + '\'\n'
    sql = sql+ ' and ( \n securityID =  \''+stock.pop(0) + '\'\n'
    
    for a in stock:
        sql = sql + ' or securityID =  \''+a + '\'\n'
    sql = sql+')'
    
    print(sql)
    mycursor.execute(sql)
            
    result = []
    for x in mycursor:
        result.append(x)
    
    
    #print(result)
    mycursor.close()
    return result


def import_line(f,name):
    mycursor = mydb.cursor()
    table = name.replace('.txt','').replace('am_','').replace('pm_','')
    read_count_max = 10
    
    #print(sql)

    i = 0
    for a in f:
        
        
        # read a line from file
        a = f.readline()
        #print(a)
        a = a[0:-1]
        #print(a)
        # split the line into tokens
        #token = re.split(r'\t+',a)
        token = re.split('\t+',a)
        #print(token)
        

        if table == 'hq_index':
            #print ('need one less element')
            token.remove('')

        sql = construct_insert_statement(table,token)
        print(sql)
        mycursor.execute(sql)
        
        
        
        
        if i==read_count_max:
            mydb.commit()
            break
        else:
            i += 1
        
    
    print("continue")

    ##    for x in f:
    ##        print(x)
    ##        
    ##        
    ##        # read a line from file
    ##        a = f.readline()
    ##        print(a)
    ##        # split the line into tokens
    ##        token = re.split(r'\t+',a)
    ##        print(token)
    
        
                
    
    ##
    ##    sql = "INSERT INTO customers (name, address) VALUES (%s, %s)"
    ##    val = ("John", "Highway 21")
    ##
    ##    sql = "Show Tabels"
    ##    mycursor.execute(sql)
    ##
    ##
    ##    #mydb.commit()
    ##
    ##    #print(mycursor.rowcount, "record inserted.")
    ##    #print("1 record inserted, ID:", mycursor.lastrowid)
    ##
    ##    #mycursor.execute("SELECT * FROM customers")
    ##
    ##    #myresult = mycursor.fetchall()
    ##
    ##    for x in mycursor:
    ##      print(x)




def construct_insert_statement(table,token):
    mycursor = mydb.cursor()

    #print(name)
    #table = name.replace('.txt','').replace('am_','').replace('pm_','')
    
    #print(table)

    sql = """
    select column_name
    from information_schema.columns
    where table_schema = 'shenzhendata'
    """
    sql = sql+ ' and table_name = \''+table + '\'\n'
    sql = sql + 'ORDER BY ORDINAL_POSITION'

    #print(sql)
    mycursor.execute(sql)

    sql2 = 'insert into '+ table + ' ('
    sql3 = ' values ('

    

    for x in mycursor:
        sql2 = sql2+x[0]+','
        #sql3 = sql3 + '%s,'
        #if not mycursor.last
        #print(x)
        #print(sql2)
        #print(sql3)

    for i in token:
        if i == 'NULL':
            sql3 = sql3+'null,'
        else:
            sql3 = sql3+'\''+str(i)+'\','

    sql2 = sql2[:-1]+')'
    sql3 = sql3[:-1]+')'
    #print(sql2)
    #print(sql3)

    sql = sql2+ sql3
    #print(sql)

    return sql
    

def import_to_db():
    
    
    flag = 0

    # prereq need to have data file in the 'raw' directory

    raw_path = "./raw"
    prepro_path = "./prepro"
    
    # Walking a directory tree and printing the names of the directories and files
    for dirpath, dirnames, files in os.walk(raw_path):
        print(f'Found directory: {dirpath}')
        for file_name in files:
            print(file_name)
            f = open(dirpath+"/"+file_name,"r")

            if flag:
                # debug mode
                for x in f:
                    print(x)
            else:
                import_line(f,file_name)
                print()
                
            
            f.close()

    


def prepro_init(raw_path,prepro_path):
    
    # delete items in the destination path
    folder = prepro_path
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print('Failed to delete %s. Reason: %s' % (file_path, e))    

def prepro_copy(raw_path,prepro_path):
    
    # copy the content from raw to preprocess folder
    src = raw_path
    dst = prepro_path
    symlinks=False
    ignore=None
    
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
    for dirpath, dirnames, files in os.walk(prepro_path):
        print(f'Found directory: {dirpath}')
        for file_name in files:
            print(file_name)
            f = open(dirpath+"/"+file_name,"r")
            
            test2(f,file_name,dirpath,dirnames,prepro_path)
                
            f.close()
    
    return

def preprocessing():

    # takes files in the raw folder and unzip into preprocessed folder
    print("assume the files are extracted into txt format")
    
    raw_path = "./raw"
    prepro_path = "./prepro"
        
    prepro_init(raw_path,prepro_path)
    prepro_copy(raw_path,prepro_path)
    prepro_concat(prepro_path)
    
def resetDB():
    global mydb
    
    mycursor = mydb.cursor()
    
    # drop database
    mycursor.execute("DROP DATABASE shenzhendata")

    # init database
    mycursor.execute("CREATE DATABASE shenzhendata")

    mydb = mysql.connector.connect(
        #connect to db
      host="127.0.0.1",
      user="root",
      passwd="pwpw",
      database="shenzhendata"
    )
    
    mycursor = mydb.cursor()
    
    # init table
    init_table(mydb) 


def dbconnect():
    global mydb
    # connect to database
    
    #resetDB()
    try:
        # try connect with shenzhendata table
        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw",
          database="shenzhendata"
        )
        #print(mydb)
        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")
        print('show all tables')
        for x in mycursor:
            print(x)
        
    except:
        # shenzhentable not setup, create shenzhen table
        
        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw"
        )
        mycursor = mydb.cursor()
        
            
        # init table
        mycursor.execute("CREATE DATABASE shenzhendata")

        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw",
          database="shenzhendata"
        )
        
        mycursor = mydb.cursor()
        
        # init table
        init_table(mydb)        
        

def init_table(mydb):
    mycursor = mydb.cursor()
    print(mycursor)
    
    #stock_status table
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
    FInancialStatus CHAR(8),
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

    
    #hq_snap_spot
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

    #hq_snap_option
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

    #hq_snap_pledge
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

    #hq_snap_bond
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

    #hq_closeSnape
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

    #snap_level_spot
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

    #snap_level_option
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

    #snap_level_pledge
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

    #snap_level_bond
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

    #hq_index
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

    #hq_order_spot
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

    #hq_order_agreement
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

    #hq_order_refinance
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

    #hq_order_pledge
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

    #hq_order_option
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

    #hq_trade_spot
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

    #hq_trade_agreement
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

    #hq_trade_refinance
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

    #hq_trade_pledge
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

    #hq_trade_option
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

def main():
    global mydb
    dbconnect()
    
    # wether to create new file to prepare
    # other way is to limit the read lines into the db
    # preprocessing()
    # currently only take 10 lines into the db for easy testing
    
    while True:
        option = input('what do you want to do? 1 import, 2 reset, 3 query, 4 kbar analysis,  5 exit\t')
        if option == '1':
            import_to_db()
        elif option == '2':
            resetDB()
        elif option == '3':
            
            # query the trade spot database
            
            # get parameter
            start = input('enter start time\t')
            end = input('enter end time\t')
            stock = input('enter stock list\t')
            column = input('column interested: 1 price, 2, volumn ')
            
            # dummy variable for test
            start= '20190102091500010'
            end= '20190102140000000'
            stock = ['002899' ,'300548','000000']   
            column = '12'
            
            result = get_dataset(start, end, stock, column)
            for a in result:
                print(a)
        elif option == '4':
            ' do the kbar analysis'
            # get parameter
            start = input('enter start time\t')
            end = input('enter end time\t')
            stock = input('enter stock list\t')
            period = input('time period (s,m,h,d,w,M) ')
            
            # dummy variable for test
            start= '2019-01-02 09:30:00'
            end= '2019-01-02 13:35:00'
            stock = ['002899' ,'300548']   
            period = 'h'
            test(start,end,stock,period)
            
        else:
            return
    
    
main()


# TODO:

# issue 2 implement kbar analyzing stuff
# issue 3 talib 
# write test class and self check


# MAJOR ISSUES


# update history

# 2020-06-10
# Issue 1 complete, may need more testing or automated testing
# issue 1 achieve single stock function, add functionality for stock list
# issue 1 add functionality to select multiple data column currently price and volumn



# 2020-06-05
# create while loop for option selection
# refreacor initialization part
# change to read 10 line of each imported file
# change table column to fit the actural import file
# hq_snap_pledge: table definition and data column does not match
# hq_snap_spot: table definition and data column does not match


# 2020-02-12 1630-1830
# adjust INT(64) into BIGINT during table setup ** for Mysql only\
# single row insert test for hq_index
# COLUMN TransactionTime need BIGINT instead of N(10)
# COLUMN TradeTime need BIGINT instead of N(10)
# using string statement to replace string 'NULL' value
# Major Problem Discovered:
#   mismatch between table definition and data
#   -hq_snap_pledge
#   -hq_snap_spot
# Temp Solution: exclude hq_snap_pledge hq_snap_spot data file during import


# Archive:


# 2020-02-11 730-930
# finish shenzhen table setup
# test initialize database and table setup
# prepare insert statement

# 2020-02-05 7am-9am
# create database shenzhendata
# setup and create table according to documentation
# up to hq_index


# 2020-02-04 6am-8am
# read file from directory
# parse file from directory
# create database, create table, test insert and select
