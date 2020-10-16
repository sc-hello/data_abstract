# 导包
import pandas as pd
import dbfread

# 设置显示的最大列数
pd.set_option('display.max_columns', 20)

# 输入设置时间间隔
flag = True
while flag:
    interval = input('提示：请输入0或1指定时间间隔，其中0代表30秒，1代表1分钟:\n')
    if interval != '0' and interval != '1':
        print("输入不合法，请重新输入!")
    else:
        flag = False

'''---------------------------------------计量数据生成------------------------------------------'''

# 导入文件的路径
source_path_prefix = './data/MeasureModel'

lst_files_tagname = [
    '2020 07 16 0000 M_RModel (Tagname).DBF',
    '2020 07 17 0000 M_RModel (Tagname).DBF',
    '2020 07 18 0000 M_RModel (Tagname).DBF',
    '2020 07 19 0000 M_RModel (Tagname).DBF',
    '2020 07 20 0000 M_RModel (Tagname).DBF',
    '2020 07 21 0000 M_RModel (Tagname).DBF',
    '2020 07 22 0000 M_RModel (Tagname).DBF',
    '2020 07 23 0000 M_RModel (Tagname).DBF',
    '2020 07 24 0000 M_RModel (Tagname).DBF',
    '2020 07 25 0000 M_RModel (Tagname).DBF',
    '2020 07 26 0000 M_RModel (Tagname).DBF',
    '2020 07 27 0000 M_RModel (Tagname).DBF',
    '2020 07 28 0000 M_RModel (Tagname).DBF',
    '2020 07 29 0000 M_RModel (Tagname).DBF',
    '2020 07 30 0000 M_RModel (Tagname).DBF',
    '2020 07 31 0000 M_RModel (Tagname).DBF',
    '2020 08 01 0000 M_RModel (Tagname).DBF',
    '2020 08 02 0000 M_RModel (Tagname).DBF',
    '2020 08 03 0000 M_RModel (Tagname).DBF',
    '2020 08 04 0000 M_RModel (Tagname).DBF',
    '2020 08 05 0000 M_RModel (Tagname).DBF',
    '2020 08 06 0000 M_RModel (Tagname).DBF',
    '2020 08 07 0000 M_RModel (Tagname).DBF',
    '2020 08 08 0000 M_RModel (Tagname).DBF',
    '2020 08 09 0000 M_RModel (Tagname).DBF',
    '2020 08 10 0000 M_RModel (Tagname).DBF',
    '2020 08 11 0000 M_RModel (Tagname).DBF',
    '2020 08 12 0000 M_RModel (Tagname).DBF',
    '2020 08 13 0000 M_RModel (Tagname).DBF',
    '2020 08 14 0000 M_RModel (Tagname).DBF',
    '2020 08 15 0000 M_RModel (Tagname).DBF',
    '2020 08 16 0000 M_RModel (Tagname).DBF',
    '2020 08 17 0000 M_RModel (Tagname).DBF',
    '2020 08 18 0000 M_RModel (Tagname).DBF',
    '2020 08 19 0000 M_RModel (Tagname).DBF',
    '2020 08 20 0000 M_RModel (Tagname).DBF',
    '2020 08 21 0000 M_RModel (Tagname).DBF',
    '2020 08 22 0000 M_RModel (Tagname).DBF',
    '2020 08 23 0000 M_RModel (Tagname).DBF',
    '2020 08 24 0000 M_RModel (Tagname).DBF',
    '2020 08 25 0000 M_RModel (Tagname).DBF',
    '2020 08 26 0000 M_RModel (Tagname).DBF',
    '2020 08 27 0000 M_RModel (Tagname).DBF'
]
lst_files_wide = [
    '2020 07 16 0000 M_RModel (Wide).DBF',
    '2020 07 17 0000 M_RModel (Wide).DBF',
    '2020 07 18 0000 M_RModel (Wide).DBF',
    '2020 07 19 0000 M_RModel (Wide).DBF',
    '2020 07 20 0000 M_RModel (Wide).DBF',
    '2020 07 21 0000 M_RModel (Wide).DBF',
    '2020 07 22 0000 M_RModel (Wide).DBF',
    '2020 07 23 0000 M_RModel (Wide).DBF',
    '2020 07 24 0000 M_RModel (Wide).DBF',
    '2020 07 25 0000 M_RModel (Wide).DBF',
    '2020 07 26 0000 M_RModel (Wide).DBF',
    '2020 07 27 0000 M_RModel (Wide).DBF',
    '2020 07 28 0000 M_RModel (Wide).DBF',
    '2020 07 29 0000 M_RModel (Wide).DBF',
    '2020 07 30 0000 M_RModel (Wide).DBF',
    '2020 07 31 0000 M_RModel (Wide).DBF',
    '2020 08 01 0000 M_RModel (Wide).DBF',
    '2020 08 02 0000 M_RModel (Wide).DBF',
    '2020 08 03 0000 M_RModel (Wide).DBF',
    '2020 08 04 0000 M_RModel (Wide).DBF',
    '2020 08 05 0000 M_RModel (Wide).DBF',
    '2020 08 06 0000 M_RModel (Wide).DBF',
    '2020 08 07 0000 M_RModel (Wide).DBF',
    '2020 08 08 0000 M_RModel (Wide).DBF',
    '2020 08 09 0000 M_RModel (Wide).DBF',
    '2020 08 10 0000 M_RModel (Wide).DBF',
    '2020 08 11 0000 M_RModel (Wide).DBF',
    '2020 08 12 0000 M_RModel (Wide).DBF',
    '2020 08 13 0000 M_RModel (Wide).DBF',
    '2020 08 14 0000 M_RModel (Wide).DBF',
    '2020 08 15 0000 M_RModel (Wide).DBF',
    '2020 08 16 0000 M_RModel (Wide).DBF',
    '2020 08 17 0000 M_RModel (Wide).DBF',
    '2020 08 18 0000 M_RModel (Wide).DBF',
    '2020 08 19 0000 M_RModel (Wide).DBF',
    '2020 08 20 0000 M_RModel (Wide).DBF',
    '2020 08 21 0000 M_RModel (Wide).DBF',
    '2020 08 22 0000 M_RModel (Wide).DBF',
    '2020 08 23 0000 M_RModel (Wide).DBF',
    '2020 08 24 0000 M_RModel (Wide).DBF',
    '2020 08 25 0000 M_RModel (Wide).DBF',
    '2020 08 26 0000 M_RModel (Wide).DBF',
    '2020 08 27 0000 M_RModel (Wide).DBF'
]

# 定义数据提取函数
def handle_jl(source_tagname, source_wide, interval=60):
    # 读取DBF文件
    dbf_tagname = dbfread.DBF(source_tagname, encoding='utf8')
    dbf_wide = dbfread.DBF(source_wide, encoding='utf8')

    # 将DBF对象转化为DataFrame对象
    df_tagname = pd.DataFrame(iter(dbf_tagname))
    df_wide = pd.DataFrame(iter(dbf_wide))

    # 获取（Tagname）文件中的Tagname列，并将series对象转化为list对象
    series_tagname_df_tagname = df_tagname.get('Tagname')
    lst_tagname_df_tagname = list(iter(series_tagname_df_tagname))

    # 获取（wide)文件中的列名,此为ndarray对象
    ar_columns_df_wide = df_wide.columns.values

    # 获取（wide)文件要删除的列的序号
    lst_drop_columns_df_wide = [ar_columns_df_wide[i] for i in [2, 3, 5, 7, 9, 11, 13, 15]]
    df_wide.drop(
        labels=lst_drop_columns_df_wide,
        axis=1,
        inplace=True
    )

    # 将（wide)文件中列名TagIndex更名为（Tagname）文件中TagIndex对应的Tagname
    dict_rename_columns_df_wide = {k: v for k, v in zip(['0', '1', '2', '3', '4', '5'], lst_tagname_df_tagname)}
    df_wide.rename(
        mapper=dict_rename_columns_df_wide,
        axis=1,
        inplace=True
    )

    # 根据时间间隔找出要删除的行
    lst_drop_rows_df_wide = []
    if interval == 60:
        for ri in range(df_wide.shape[0]):
            if df_wide.at[ri, 'Time'][-2:] not in ['00']:
                lst_drop_rows_df_wide.append(ri)
    elif interval == 30:
        for ri in range(df_wide.shape[0]):
            if df_wide.at[ri, 'Time'][-2:] not in ['00', '30']:
                lst_drop_rows_df_wide.append(ri)

    # 删除行
    df_wide.drop(
        labels=lst_drop_rows_df_wide,
        axis=0,
        inplace=True
    )
    # 返回处理后的数据
    return df_wide

# 计量1支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(1) + '/' + t
    source_wide = source_path_prefix + str(1) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-204\VALUE':'PT-204','FIQ-201\FT_VALUE':'FIQ-201','TT\\204':'TT-204'},inplace=True)
f_sum = f_sum[['Date','Time','PT-204','FIQ-201','TT-204']]
target = './output/计量' + str(1) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')

# 计量2支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(2) + '/' + t
    source_wide = source_path_prefix + str(2) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-205\VALUE':'PT-205','FIQ-202\FT_VALUE':'FIQ-202','TT\\205':'TT-205'},inplace=True)
f_sum = f_sum[['Date','Time','PT-205','FIQ-202','TT-205']]
target = './output/计量' + str(2) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')

# 计量3支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(3) + '/' + t
    source_wide = source_path_prefix + str(3) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-206\VALUE':'PT-206','FIQ-203\FT_VALUE':'FIQ-203','TT\\206':'TT-206'},inplace=True)
f_sum = f_sum[['Date','Time','PT-206','FIQ-203','TT-206']]
target = './output/计量' + str(3) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')

# 计量4支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(4) + '/' + t
    source_wide = source_path_prefix + str(4) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-207\VALUE':'PT-207','FIQ-204\FT_VALUE':'FIQ-204','TT\\207':'TT-207'},inplace=True)
f_sum = f_sum[['Date','Time','PT-207','FIQ-204','TT-207']]
target = './output/计量' + str(4) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')

# 计量5支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(5) + '/' + t
    source_wide = source_path_prefix + str(5) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-301A\VALUE':'PT-301A','FIQ-301A\FT_VALUE':'FIQ-301A','TT\\301A':'TT-301A'},inplace=True)
f_sum = f_sum[['Date','Time','PT-301A','FIQ-301A','TT-301A']]
target = './output/计量' + str(5) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')

# 计量6支路
f_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + str(6) + '/' + t
    source_wide = source_path_prefix + str(6) + '/' + w
    df_wide = handle_jl(source_tagname, source_wide, interval)
    f_sum = pd.concat([f_sum, df_wide])
f_sum.rename(columns={'PT-301B\VALUE':'PT-301B','FIQ-301B\FT_VALUE':'FIQ-301B','TT\\301B':'TT-301B'},inplace=True)
f_sum = f_sum[['Date','Time','PT-301B','FIQ-301B','TT-301B']]
target = './output/计量' + str(6) + '支路.csv'
f_sum.to_csv(target, index=False, encoding='utf8', line_terminator='\n')


'''---------------------------------------调压数据生成------------------------------------------'''

# 按调压支路需要的格式合并计量1——6支路的数据
f_sums = []
for i in range(1, 7):
    f_sum = pd.DataFrame()
    for t, w in zip(lst_files_tagname, lst_files_wide):
        source_tagname = source_path_prefix + str(i) + '/' + t
        source_wide = source_path_prefix + str(i) + '/' + w
        df_wide = handle_jl(source_tagname, source_wide, interval)
        f_sum = pd.concat([f_sum, df_wide])
    f_sums.append(f_sum)

# 汇总调压数据
source_path_prefix = './data/Flow_PressModel/'
lst_files_tagname = [
    '2020 07 16 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 17 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 18 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 19 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 20 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 21 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 22 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 23 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 24 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 25 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 26 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 27 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 28 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 29 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 30 0000 Flow_PressModelData (Tagname).DBF',
    '2020 07 31 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 01 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 02 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 03 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 04 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 05 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 06 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 07 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 08 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 09 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 10 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 11 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 12 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 13 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 14 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 15 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 16 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 17 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 18 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 19 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 20 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 21 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 22 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 23 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 24 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 25 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 26 0000 Flow_PressModelData (Tagname).DBF',
    '2020 08 27 0000 Flow_PressModelData (Tagname).DBF',
]

lst_files_wide = [
    '2020 07 16 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 17 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 18 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 19 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 20 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 21 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 22 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 23 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 24 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 25 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 26 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 27 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 28 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 29 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 30 0000 Flow_PressModelData (Wide).DBF',
    '2020 07 31 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 01 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 02 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 03 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 04 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 05 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 06 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 07 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 08 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 09 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 10 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 11 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 12 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 13 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 14 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 15 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 16 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 17 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 18 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 19 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 20 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 21 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 22 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 23 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 24 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 25 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 26 0000 Flow_PressModelData (Wide).DBF',
    '2020 08 27 0000 Flow_PressModelData (Wide).DBF'
]

# 定义数据提取程序
def handle_ty(source_tagname, source_wide, interval):
    # 读取DBF文件
    dbf_tagname = dbfread.DBF(source_tagname, encoding='utf8')
    dbf_wide = dbfread.DBF(source_wide, encoding='utf8')

    # 将DBF对象转化为DataFrame对象
    df_tagname = pd.DataFrame(iter(dbf_tagname))
    df_wide = pd.DataFrame(iter(dbf_wide))

    # 获取（Tagname）文件中的Tagname列，并将series对象转化为list对象
    series_tagname_df_tagname = df_tagname.get('Tagname')
    lst_tagname_df_tagname = list(iter(series_tagname_df_tagname))

    # 获取（wide)文件中的列名,此为ndarray对象
    ar_columns_df_wide = df_wide.columns.values
    # 获取（wide)文件要删除的列的序号
    lst_drop_columns_df_wide = [ar_columns_df_wide[i] for i in
                                [2, 3, 5, 7, 9, 11, 13, 15, 17, 19, 21, 23, 25, 27, 29, 31, 33, 35, 37, 39, 41]]
    df_wide.drop(
        labels=lst_drop_columns_df_wide,
        axis=1,
        inplace=True
    )

    # 将（wide)文件中列名TagIndex更名为（Tagname）文件中TagIndex对应的Tagname
    dict_rename_columns_df_wide = {k: v for k, v in
                                   zip(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12',
                                        '13', '14', '15', '16', '17', '18'], lst_tagname_df_tagname)}
    df_wide.rename(
        mapper=dict_rename_columns_df_wide,
        axis=1,
        inplace=True
    )

    # 根据时间间隔找出要删除的行
    lst_drop_rows_df_wide = []
    if interval == 60:
        for ri in range(df_wide.shape[0]):
            if df_wide.at[ri, 'Time'][-2:] not in ['00']:
                lst_drop_rows_df_wide.append(ri)
    elif interval == 30:
        for ri in range(df_wide.shape[0]):
            if df_wide.at[ri, 'Time'][-2:] not in ['00', '30']:
                lst_drop_rows_df_wide.append(ri)

    # 删除行
    df_wide.drop(
        labels=lst_drop_rows_df_wide,
        axis=0,
        inplace=True
    )
    # 返回处理后的数据
    return df_wide

# 执行数据提取程序
t_sum = pd.DataFrame()
for t, w in zip(lst_files_tagname, lst_files_wide):
    source_tagname = source_path_prefix + t
    source_wide = source_path_prefix + w
    df_wide = handle_ty(source_tagname, source_wide, interval)
    t_sum = pd.concat([t_sum, df_wide])

# 调压1支路
t_01 = pd.merge(t_sum, f_sums[0], how='inner', on=['Date', 'Time'])
t_01 = t_01[['Date', 'Time', 'FIQ-201\FT_VALUE_x', 'PT-208\VALUE', 'RMG_1\FAWEI_KAIDU', 'PT-212\VALUE_y']]
t_01.columns = ['Date', 'Time', 'FIQ-201', 'PT-208', 'RMG_1', 'PT-212']
t_01.to_csv('./output/调压1支路.csv', index=False)

# 调压2支路
t_02 = pd.merge(t_sum, f_sums[1], how='inner', on=['Date', 'Time'])
t_02 = t_02[['Date', 'Time', 'FIQ-202\FT_VALUE_x', 'PT-209\VALUE', 'RMG_2\FAWEI_KAIDU', 'PT-213\VALUE_y']]
t_02.columns = ['Date', 'Time', 'FIQ-202', 'PT-209', 'RMG_2', 'PT-213']
t_02.to_csv('./output/调压2支路.csv', index=False)

# 调压3支路
t_03 = pd.merge(t_sum, f_sums[2], how='inner', on=['Date', 'Time'])
t_03 = t_03[['Date', 'Time', 'FIQ-203\FT_VALUE_x', 'PT-210\VALUE', 'RMG_3\FAWEI_KAIDU', 'PT-214\VALUE_y']]
t_03.columns = ['Date', 'Time', 'FIQ-203', 'PT-210', 'RMG_3', 'PT-214']
t_03.to_csv('./output/调压3支路.csv', index=False)

# 调压4支路
t_04 = pd.merge(t_sum, f_sums[3], how='inner', on=['Date', 'Time'])
t_04 = t_04[['Date', 'Time', 'FIQ-204\FT_VALUE_x', 'PT-211\VALUE', 'RMG_4\FAWEI_KAIDU', 'PT-215\VALUE_y']]
t_04.columns = ['Date', 'Time', 'FIQ-204', 'PT-211', 'RMG_4', 'PT-215']
t_04.to_csv('./output/调压4支路.csv', index=False)

# 调压5支路
t_05 = pd.merge(t_sum, f_sums[4], how='inner', on=['Date', 'Time'])
t_05 = t_05[['Date', 'Time', 'FIQ-301A\FT_VALUE_x', 'PT-302A\VALUE', 'RMG_5\FAWEI_KAIDU', 'PT-301A\VALUE_y']]
t_05.columns = ['Date', 'Time', 'FIQ-301A', 'PT-302A', 'RMG_5', 'PT-301A']
t_05.to_csv('./output/调压5支路.csv', index=False)

# 调压6支路
t_06 = pd.merge(t_sum, f_sums[5], how='inner', on=['Date', 'Time'])
t_06 = t_06[['Date', 'Time', 'FIQ-301B\FT_VALUE_x', 'PT-303A\VALUE', 'RMG_6\FAWEI_KAIDU', 'PT-301B\VALUE_y']]
t_06.columns = ['Date', 'Time', 'FIQ-301B', 'PT-303A', 'RMG_6', 'PT-301B']
t_06.to_csv('./output/调压6支路.csv', index=False)

# 灞桥站秦华用气负载
t_qh = t_sum[['Date', 'Time', 'FIQ-201\FT_VALUE', 'FIQ-202\FT_VALUE', 'FIQ-203\FT_VALUE', 'FIQ-204\FT_VALUE',
              'FIQ-301A\FT_VALUE', 'FIQ-301B\FT_VALUE', 'PT-216\VALUE']]
t_qh.columns = ['Date', 'Time', 'FIQ-201', 'FIQ-202', 'FIQ-203', 'FIQ-204', 'FIQ-301A', 'FIQ-301B', 'PT-216']
t_qh.to_csv('./output/灞桥站秦华用气负载.csv', index=False)
