import mysql.connector
from zipfile import ZipFile
import os
import shutil
import re


def test2(f, name,dirpath,dirname,destpath):
    # this test2 is used to make shorter version of the input file used in the analysis 
    # easier for testing things
    # take raw input and give out a concatenated version of the file
    # put into preprocess folder
    
    return

    
    
    


def test(f,name):
    mycursor = mydb.cursor()
    table = name.replace('.txt','').replace('am_','').replace('pm_','')
    
    
    #print(sql)

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
        
    
    mydb.commit()
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
                test(f,file_name)
                print()
                
            
            f.close()

    


def preprocessing():

    raw_path = "./raw"
    prepro_path = "./prepro"
        
    # takes files in the raw folder and unzip into preprocessed folder
    print("assume the files are extracted into txt format")
    
    
    
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
    
    
    # Walking a directory tree and printing the names of the directories and files
    for dirpath, dirnames, files in os.walk(raw_path):
        print(f'Found directory: {dirpath}')
        for file_name in files:
            print(file_name)
            f = open(dirpath+"/"+file_name,"r")
            
            test2(f,file_name,dirpath,dirnames,prepro_path)
                
            f.close()

    

def resetDB():
    try:
        print("reset DB")
        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw",
          database="shenzhendata"
        )
        print(mydb)
        mycursor = mydb.cursor()
    
        
    except:
        print("no need for reset")

    ##check with user if want full initialization
    reset = input('Reset Database (y/n)')
    if reset == "y":
        mycursor.execute("DROP DATABASE shenzhendata")
        mydb.close()


def dbconnect():
    # connect to database
    global mydb
    resetDB()
    try:
        print("regular dbconnect")
        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw",
          database="shenzhendata"
        )
        print(mydb)
        mycursor = mydb.cursor()
        mycursor.execute("SHOW TABLES")
        for x in mycursor:
            print(x)
        
    except:
        # mydatabase not setup, need full init
        print("dbconnect except")
        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw"
        )
        print(mydb)
        mycursor = mydb.cursor()
        
            
        # init database
        mycursor.execute("CREATE DATABASE shenzhendata")

        mydb = mysql.connector.connect(
            #connect to db
          host="127.0.0.1",
          user="root",
          passwd="pwpw",
          database="shenzhendata"
        )
        print(mydb)
        mycursor = mydb.cursor()
        
        # init table
        init_table(mycursor)
        
    print("end dbconnect")

def init_table(mycursor):
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
    print("the main")
    dbconnect()
    preprocessing()
    import_to_db()
    
    
main()


# update history

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

# TODO:
# continue prepare insert statement for entire file
# make insert and select test
# create user interface to import and specify range of select
# create function to handel zip file in preprocessing

# MAJOR ISSUES
# hq_snap_pledge: table definition and data column does not match
#   definition: 10 columns after TradingPhaseCode
#   data: 13 columns after TradingPhaseCode
# hq_snap_spot: table definition and data column does not match
#   definition: 10 columns after TradingPhaseCode
#   data: 13 columns after TradingPhaseCode




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




