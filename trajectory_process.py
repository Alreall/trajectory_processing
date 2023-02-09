import pandas as pd
import os
import re
import csv
import math
from tqdm import tqdm
# 读取文件
# 提取出每一时刻的车辆的状态信息
def time_group(path):
    df = pd.read_csv('reconstraction.csv')
    # 按照time进行分组，并转换为列表形式
    timelist = list(df.groupby(['time_s']))
    # print(timelist)
    # 遍历列表
    for time in timelist:
        time_pd = pd.DataFrame(time[1])
        time_pd.to_csv(path+'/'+str(time[0])+'.csv')

# 对数据进行处理，标定车辆的前后车
def handle(path,path1):
    # listdir()--返回path指定 的 文件夹中包含的文件或者文件夹名字 的 列表
    FileNames = os.listdir(path)    # 因此Filename是一个列表
    for fn in FileNames:
        if re.search(r'\.csv$', fn):
            file = os.path.join(path, fn)
            # df = pd.read_csv(file,encoding='utf-8')
            # print(fn)  # 文件名
            f = open(file,'r')
            reader = csv.reader(f)
            df = pd.read_csv(file,index_col='tra_ID')   # 选择tra_ID作为行索引
            # m用于存储每一行的元素
            m = []
            for row in reader:
                m.append(row)
            # 获取行的个数
            x = len(m)
            aa = {}  # 字典，用于表示对应编号的前后车
            y = df['x']
            z = list(y)
            for i in range(1, x):
                subOD = []  # sub outputdata
                a1 = []
                a2 = []
                b = []
                temf = []  # 存储后方车辆的距离
                temfdic = []  # 存储后方车辆的编号
                teml = []  # 存储前方车辆的距离
                temldic = []  # 存储前方车辆的编号
                for j in range(1, x):
                    deltX = float(m[i][6]) - float(m[j][6])  # 第i行，第1列的元素，对应的x坐标
                    deltY = float(m[i][7]) - float(m[j][7])
                    # deltZ = float(m[i][9]) - float(m[j][9])
                    # 求车辆之间的距离信息
                    dis = math.sqrt((deltX ** 2) + (deltY ** 2))  # 计算距离

                    x1 = m[i][2]
                    x2 = m[j][2]
                    # 如果距离不等于0，大于它的就是前车，小于它的就是后车
                    # if dis != 0:
                    if df.loc[int(x2), 'x'] < df.loc[int(x1), 'x']:
                        teml.append(dis)
                        temldic.append(x2)
                    elif df.loc[int(x2), 'x'] >= df.loc[int(x1), 'x']:
                        temf.append(dis)
                        temfdic.append(x2)
                    '''
                    出现空值的情况是，如果这是头车，那么就不存在比他大的了，那么在这一时刻她就是空值
                    '''
                '''
                如果teml不是空，那么就存储到hebingl里面，添加一个b
                如果不设置else，那么就只存储了一个b
                '''
                # 如果都不是空的话
                # subOD存储的是前后车的一个编号
                if teml and len(temf) != 1:
                    hebingl = dict(zip(teml, temldic))  # 存储前方车辆的字典
                    for t1 in hebingl.keys():  # 遍历距前方车辆的距离
                        a1.append(t1)
                    c1 = min(a1)  # 距离最近的前方车辆
                    b.append(c1)
                    subOD.append(hebingl[b[0]])  # 添加第一个元素,前车
                    hebingf = dict(zip(temf, temfdic))  # 存储后方车辆的字典
                    for t2 in hebingf.keys():  # 遍历距后方车辆的距离
                        a2.append(t2)
                    a2.remove(0)
                    c2 = min(a2)  # 距离最近的后方车辆
                    b.append(c2)
                    subOD.append((hebingf[b[1]]))  # 添加第二个元素，后车
                # 如果前方是空，后方不是空，就前方的添加None，后方添加车辆
                elif not teml and len(temf) != 1:
                    subOD.append(9999)
                    hebingf = dict(zip(temf, temfdic))  # 存储后方车辆的字典
                    for ii in hebingf.keys():  # 遍历距后方车辆的距离
                        a2.append(ii)
                    a2.remove(0)
                    c2 = min(a2)
                    b.append(c2)
                    subOD.append((hebingf[b[0]]))  # 添加第二个元素，后车
                # 如果后方是空，前方不是空，就后方的添加None，前方添加车辆
                elif teml and len(temf) == 1:
                    hebingl = dict(zip(teml, temldic))  # 存储前方车辆的字典
                    for ii in hebingl.keys():  # 遍历距前方车辆的距离
                        a1.append(ii)
                    c1 = min(a1)
                    b.append(c1)
                    subOD.append(hebingl[b[0]])  # 添加第一个元素,前车
                    subOD.append(9999)
                if subOD:
                    bb = {m[i][2]: subOD}  # 车辆对应的前后车的字典情况
                    aa.update(bb)  # 存储车辆的前后车的字典

            # 遍历的是按照顺序的，所以aa里面存储的也是按照顺序来的
            df.loc[:, 'lead'] = 0
            df.loc[:, 'follow'] = 0
            for key, value in aa.items():
                if df.loc[int(key), 'x'] == min(z):
                    df.loc[int(key), 'lead'] = None
                    df.loc[int(key), 'follow'] = value[1]
                elif df.loc[int(key), 'x'] == max(z):
                    df.loc[int(key), 'lead'] = value[0]
                    df.loc[int(key), 'follow'] = None
                else:
                    df.loc[int(key), 'lead'] = value[0]
                    df.loc[int(key), 'follow'] = value[1]
            df.to_csv(path1+'/'+fn)

# 将CSV文件进行合并
def get_data(path):
    df_list = []
    for file in tqdm(os.listdir(path)):  ## 进度条
        file_path = os.path.join(path, file)
        df = pd.read_csv(file_path)
        df_list.append(df)
    df = pd.concat(df_list)
    return df
# cPath = '/Users/yang/Downloads/trajectory_process-main/data1'
#     test_df = get_data(path)
#     test_df.to_csv(path_or_buf="final.csv",index=False)         # 保存为CSV文件

path = r"/Users/yang/Downloads/trajectory_process-main/data"  # 读取csv文件目录路径
path1 = r'/Users/yang/Downloads/trajectory_process-main/data1'
time_group(path)
handle(path,path1)
test_df = get_data(path)
test_df.to_csv(path_or_buf="final.csv",index=False)         # 保存为CSV文件
