import pyodbc
import pandas as pd

# 开启操作数据库的程序
DBfile = r"C:\Users\scsbt\Desktop\天然气项目\天然气数据（2020-08-29 ）\未央\ModlesDataACDB.mdb"
conn = pyodbc.connect(r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=" + DBfile + ";Uid=;Pwd=;")
cursor = conn.cursor()

# 数据库中包含的表
'''
t1 = 'JX1_QH2_Model_FloatTable'----------{3,8,21,22,23,24}
t2 = 'JX1_QH3_Model_FloatTable'----------{4,9,25,26,27,28}
t3 = 'JX1_ZS1_Model_FloatTable'----------{29,30,31,32,33,34,35,36}
t4 = 'JX2_QH1_Model_FloatTable'----------{0,5,37}
t5 = 'JX2_QH2_Model_FloatTable'----------{1,6,38}
t6 = 'JX2_QH3_Model_FloatTable'----------{2,7,39}
t7 = 'Models_StringTable'
t8 = 'Models_TagTable'
t9 = 'PRE_REG_Model_FloatTable'----------{0,1,2,5,6,7,11,12,13,14,15,16,17,18,19,20}
t10 = 'FLW_PRE_Model_FloatTable'----------{0,1,2,3,4,5,6,7,8,9,10,11}
'''

# 定义数据提取函数
def handle(interval):

    if interval=='0':
        addcon_jx1_qh1 = "(T1.DateAndTime LIKE'%00' OR T1.DateAndTime LIKE'%30')"
        addcon_jx1_qh2 = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_jx1_zs = "(T1.DateAndTime LIKE'%00' OR T1.DateAndTime LIKE'%30')"
        addcon_jx2_jl1 = "(T1.DateAndTime LIKE'%00' OR T1.DateAndTime LIKE'%30')"
        addcon_jx2_jl2 = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_jx2_jl3 = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_jx2_ty = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_jx1_qh = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_jx2_yq = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"
        addcon_wyz_qh = "(T1.DateAndTime LIKE'%29' OR T1.DateAndTime LIKE'%59')"

    elif interval=='1':
        addcon_jx1_qh1 = "T1.DateAndTime LIKE'%00'"
        addcon_jx1_qh2 = "T1.DateAndTime LIKE'%59'"
        addcon_jx1_zs = "T1.DateAndTime LIKE'%00'"
        addcon_jx2_jl1 = "T1.DateAndTime LIKE'%00'"
        addcon_jx2_jl2 = "T1.DateAndTime LIKE'%59'"
        addcon_jx2_jl3 = "T1.DateAndTime LIKE'%59'"
        addcon_jx2_ty = "T1.DateAndTime LIKE'%59'"
        addcon_jx1_qh = "T1.DateAndTime LIKE'%59'"
        addcon_jx2_yq = "T1.DateAndTime LIKE'%59'"
        addcon_wyz_qh = "T1.DateAndTime LIKE'%59'"

#-------------------------靖西1线秦华1支路

    query_jx1_qh1 = '''
                    SELECT T1.DateAndTime, T1.Val as PI_1002 ,T2.Val as FI_1002,T3.Val as FT_1002,
                    T4.Val as TI_1002 ,T5.Val as FCV_1002,T6.Val as OFD_1002
    
                    FROM JX1_QH2_Model_FloatTable T1,JX1_QH2_Model_FloatTable T2,JX1_QH2_Model_FloatTable T3,
                    JX1_QH2_Model_FloatTable T4,JX1_QH2_Model_FloatTable T5,JX1_QH2_Model_FloatTable T6
    
                    WHERE T1.TagIndex=3 AND T2.TagIndex=8 AND T3.TagIndex=21
                    AND T4.TagIndex=22 AND T5.TagIndex=23 AND T6.TagIndex=24
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime AND T1.DateAndTime=T4.DateAndTime
                    AND T1.DateAndTime=T5.DateAndTime AND T1.DateAndTime=T6.DateAndTime
                    AND 
                    ''' + addcon_jx1_qh1

    # 执行sql查询语句
    result_jx1_qh1 = pd.read_sql(query_jx1_qh1, conn)

    # 输出为csv文件
    result_jx1_qh1.to_csv('output\靖西1线秦华1支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西1线秦华2支路

    query_jx1_qh2 = '''
                    SELECT T1.DateAndTime, T1.Val as PI_1003 ,T2.Val as FI_1003,T3.Val as FT_1003,
                    T4.Val as TI_1003 ,T5.Val as FCV_1003,T6.Val as OFD_1003
    
                    FROM JX1_QH3_Model_FloatTable T1,JX1_QH3_Model_FloatTable T2,JX1_QH3_Model_FloatTable T3,
                    JX1_QH3_Model_FloatTable T4,JX1_QH3_Model_FloatTable T5,JX1_QH3_Model_FloatTable T6
    
                    WHERE T1.TagIndex=4 AND T2.TagIndex=9 AND T3.TagIndex=27
                    AND T4.TagIndex=26 AND T5.TagIndex=28 AND T6.TagIndex=25
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime AND T1.DateAndTime=T4.DateAndTime
                    AND T1.DateAndTime=T5.DateAndTime AND T1.DateAndTime=T6.DateAndTime
                    AND 
                    ''' + addcon_jx1_qh2

    # 执行sql查询语句
    result_jx1_qh2 = pd.read_sql(query_jx1_qh2, conn)

    # 输出为csv文件
    result_jx1_qh2.to_csv('output\靖西1线秦华2支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西1线正尚支路-----------------------------------

    query_jx1_zs = '''
                    SELECT T1.DateAndTime, T1.Val as PI_101 ,T2.Val as FI_101,T3.Val as FT_101,
                    T4.Val as TI_101 ,T5.Val as FCV_101,T6.Val as OFD_101
    
                    FROM JX1_ZS1_Model_FloatTable T1,JX1_ZS1_Model_FloatTable T2,JX1_ZS1_Model_FloatTable T3,
                    JX1_ZS1_Model_FloatTable T4,JX1_ZS1_Model_FloatTable T5,JX1_ZS1_Model_FloatTable T6
    
                    WHERE T1.TagIndex=29 AND T2.TagIndex=32 AND T3.TagIndex=30
                    AND T4.TagIndex=34 AND T5.TagIndex=33 AND T6.TagIndex=31
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime AND T1.DateAndTime=T4.DateAndTime
                    AND T1.DateAndTime=T5.DateAndTime AND T1.DateAndTime=T6.DateAndTime
                    AND 
                    ''' + addcon_jx1_zs

    # 执行sql查询语句
    result_jx1_zs = pd.read_sql(query_jx1_zs, conn)

    # 输出为csv文件
    result_jx1_zs.to_csv('output\靖西1线正尚支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西2线计量1支路-----------------------------------

    query_jx2_jl1 = '''
                    SELECT T1.DateAndTime, T1.Val as PI_XA101 ,T2.Val as FI_XA101,T3.Val as TI_XA101

                    FROM JX2_QH1_Model_FloatTable T1,JX2_QH1_Model_FloatTable T2,JX2_QH1_Model_FloatTable T3

                    WHERE T1.TagIndex=0 AND T2.TagIndex=5 AND T3.TagIndex=37
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime
                    AND 
                    ''' + addcon_jx2_jl1

    # 执行sql查询语句
    result_jx2_jl1 = pd.read_sql(query_jx2_jl1, conn)

    # 输出为csv文件
    result_jx2_jl1.to_csv('output\靖西2线计量1支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西2线计量2支路-----------------------------------

    query_jx2_jl2 = '''
                    SELECT T1.DateAndTime, T1.Val as PI_XA102 ,T2.Val as FI_XA102,T3.Val as TI_XA102
    
                    FROM JX2_QH2_Model_FloatTable T1,JX2_QH2_Model_FloatTable T2,JX2_QH2_Model_FloatTable T3
    
                    WHERE T1.TagIndex=1 AND T2.TagIndex=6 AND T3.TagIndex=38
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime
                    AND 
                    ''' + addcon_jx2_jl2

    # 执行sql查询语句
    result_jx2_jl2 = pd.read_sql(query_jx2_jl2, conn)

    # 输出为csv文件
    result_jx2_jl2.to_csv('output\靖西2线计量2支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西2线计量3支路-----------------------------------

    query_jx2_jl3 = '''
                    SELECT T1.DateAndTime, T1.Val as PI_XA103 ,T2.Val as FI_XA103,T3.Val as TI_XA103
    
                    FROM JX2_QH3_Model_FloatTable T1,JX2_QH3_Model_FloatTable T2,JX2_QH3_Model_FloatTable T3
    
                    WHERE T1.TagIndex=2 AND T2.TagIndex=7 AND T3.TagIndex=39
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime
                    AND 
                    ''' + addcon_jx2_jl3

    # 执行sql查询语句
    result_jx2_jl3 = pd.read_sql(query_jx2_jl3, conn)

    # 输出为csv文件
    result_jx2_jl3.to_csv('output\靖西2线计量3支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西2线调压支路-----------------------------------

    query_jx2_ty = '''
                    SELECT T1.DateAndTime, T1.Val as PI_XA101 ,T2.Val as PI_XA102,T3.Val as PI_XA103,
                    T4.Val as FI_XA101 ,T5.Val as FI_XA102,T6.Val as FI_XA103,
                    T7.Val as PI_XA105 ,T8.Val as PI_XA106,T9.Val as PI_XA107
    
                    FROM PRE_REG_Model_FloatTable T1,PRE_REG_Model_FloatTable T2,PRE_REG_Model_FloatTable T3,
                    PRE_REG_Model_FloatTable T4,PRE_REG_Model_FloatTable T5,PRE_REG_Model_FloatTable T6,
                    PRE_REG_Model_FloatTable T7,PRE_REG_Model_FloatTable T8,PRE_REG_Model_FloatTable T9
    
                    WHERE T1.TagIndex=0 AND T2.TagIndex=1 AND T3.TagIndex=2 
                    AND T4.TagIndex=5 AND T5.TagIndex=6 AND T6.TagIndex=7 
                    AND T7.TagIndex=12 AND T8.TagIndex=13 AND T9.TagIndex=14
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime
                    AND T1.DateAndTime=T4.DateAndTime AND T1.DateAndTime=T5.DateAndTime
                    AND T1.DateAndTime=T6.DateAndTime AND T1.DateAndTime=T7.DateAndTime
                    AND T1.DateAndTime=T8.DateAndTime AND T1.DateAndTime=T9.DateAndTime
                    AND 
                    ''' + addcon_jx2_ty

    # 执行sql查询语句
    result_jx2_ty = pd.read_sql(query_jx2_ty, conn)

    # 输出为csv文件
    result_jx2_ty.to_csv('output\靖西2线调压支路.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西1线秦华用气负载-----------------------------------

    query_jx1_qh = '''
                    SELECT T1.DateAndTime, T1.Val as FI_1002 ,T2.Val as FI_1003,T3.Val as PI_1008
    
                    FROM FLW_PRE_Model_FloatTable T1,FLW_PRE_Model_FloatTable T2,FLW_PRE_Model_FloatTable T3
    
                    WHERE T1.TagIndex=8 AND T2.TagIndex=9 AND T3.TagIndex=10
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime
                    AND
                    ''' + addcon_jx1_qh

    # 执行sql查询语句
    result_jx1_qh = pd.read_sql(query_jx1_qh, conn)

    # 输出为csv文件
    result_jx1_qh.to_csv('output\靖西1线秦华用气负载.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------靖西2线用气负载-----------------------------------

    query_jx2_yq = '''
                    SELECT T1.DateAndTime, T1.Val as FI_XA101, T2.Val as FI_XA102, T3.Val as FI_XA103, T4.Val as PI_XA108
    
                    FROM FLW_PRE_Model_FloatTable T1, FLW_PRE_Model_FloatTable T2,
                    FLW_PRE_Model_FloatTable T3, FLW_PRE_Model_FloatTable T4
    
                    WHERE T1.TagIndex=5 AND T2.TagIndex=6 AND T3.TagIndex=7 AND T4.TagIndex=11
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime AND T1.DateAndTime=T4.DateAndTime
                    AND 
                    ''' + addcon_jx2_yq

    # 执行sql查询语句
    result_jx2_yq = pd.read_sql(query_jx2_yq, conn)

    # 输出为csv文件
    result_jx2_yq.to_csv('output\靖西2线用气负载.csv', index=False, encoding='utf8', line_terminator='\n')

#-------------------------未央站秦华用气负载-----------------------------------

    query_wyz_qh = '''
                    SELECT T1.DateAndTime, T1.Val as FI_1002, T2.Val as FI_1003, T3.Val as PI_1008, T4.Val as FI_XA101,
                    T5.Val as FI_XA102, T6.Val as FI_XA103, T7.Val as PI_XA108
    
                    FROM FLW_PRE_Model_FloatTable T1, FLW_PRE_Model_FloatTable T2,FLW_PRE_Model_FloatTable T3, 
                    FLW_PRE_Model_FloatTable T4, FLW_PRE_Model_FloatTable T5, FLW_PRE_Model_FloatTable T6, 
                    FLW_PRE_Model_FloatTable T7
    
                    WHERE T1.TagIndex=8 AND T2.TagIndex=9 AND T3.TagIndex=10 AND T4.TagIndex=5
                    AND T5.TagIndex=6 AND T6.TagIndex=7 AND T7.TagIndex=11 
                    AND T1.DateAndTime=T2.DateAndTime AND T1.DateAndTime=T3.DateAndTime AND T1.DateAndTime=T4.DateAndTime
                    AND T1.DateAndTime=T5.DateAndTime AND T1.DateAndTime=T6.DateAndTime AND T1.DateAndTime=T7.DateAndTime
                    AND 
                    ''' + addcon_wyz_qh

    # 执行sql查询语句
    result_wyz_qh = pd.read_sql(query_wyz_qh, conn)

    # 输出为csv文件
    result_wyz_qh.to_csv('output\未央站秦华用气负载.csv', index=False, encoding='utf8', line_terminator='\n')

# 输入时间间隔并执行数据提取函数
flag = True
while flag:
    interval = input('提示：请输入0或1指定时间间隔，其中0代表30秒，1代表1分钟:\n')
    if interval != '0' and interval != '1':
        print("输入不合法，请重新输入!")
    else:
        flag = False
handle(interval)

# 关闭操作数据库的程序
cursor.close()
conn.close()