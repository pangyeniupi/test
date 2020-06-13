import requests,os,datetime,re,time,sys,random
import json
from bs4 import BeautifulSoup
import pymysql
from log import Logger


def get_data(url,station_id,station_name):
    # print('开始下载%s数据'%station_name)
    print('%s,向网站发起请求~~~~~~~~~~'%datetime.datetime.now().strftime(r"%Y/%m/%d %H:%M:%S"))

    header={
    'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36'
    }
    # url='https://www.cwb.gov.tw/V8/C/M/OBS_Marine/48hrsSeaObs_MOD/M46761F.html?'
    #url='https://www.cwb.gov.tw/V8/C/M/OBS_Marine48hrsSeaObs_MOD/M46761F.html?'
    print(url)
    try:
        res=requests.get(url,headers=header,timeout=(20,180))
        print('开始解析%s网页数据~~~~~~~'%station_name)
        res.encoding='utf-8'
        res=res.text.replace('<br />','')
        soup=BeautifulSoup(res,'html.parser')
        # 时间获取
        dates=[]
        times=[]
        for th in soup.find_all('th'):    
            dates.append(str(datetime.datetime.now().year)+'/'+th.string.strip().split("(")[0])#日期
            times.append(th.string.strip().split("(")[1].split(')')[-1])#时间
        # print(times)
        tides=[]
        waves=[]
        waveDir=[]
        wave_T=[]
        wind=[]
        windScale=[]#风级
        windDir=[]
        #阵风,风级
        gustiness=[]
        gustinessScale=[]
        seaTem=[]
        airTem=[]
        air_pre=[]#气压
        oceanCurrent=[]#海流
        oceanCurrentDir=[]

        trs=soup.find_all('tr')
        # 判断空值？
        def empty(x):
            if x is None or x=='-':
                return 'null'
            else:
                return x
        for tr in trs:
            tds=tr.find_all('td')     
            # 潮高
            wl=[]
            for i in tds[0].ul.children:
                wl.append(i.string)
            tides.append(empty(wl[3]))
            # 浪高
            waves.append(empty(tds[1].string))
            # 浪向a
            w=tds[2].find(re.compile('<span class="sr-only">(.*?)</span>'))
            waveDir.append(empty(w))
            # 浪周期
            wave_T.append(empty(tds[3].string))
            #风速 风级
            wl=[]
            for i in tds[4].contents:
                wl.append(i.string)
            wind.append(empty(wl[1]))
            windScale.append(empty(wl[3]))
            # 风向
            w=tds[5].find(re.compile('<span class="sr-only">(.*?)</span>'))
            windDir.append(empty(w))
            #阵风\级
            wl=[]
            for i in tds[6].contents:
                wl.append(i.string)
            gustiness.append(empty(wl[1]))
            gustinessScale.append(empty(wl[3]))
            # 海温
            seaTem.append(empty(tds[7].find(class_="tempC").string))
            #气温
            airTem.append(empty(tds[8].find(class_="tempC").string))
            #气压
            air_pre.append(empty(tds[9].string))
            #海流流向
            w=tds[10].find(re.compile('<span class="sr-only">(.*?)</span>'))
            oceanCurrentDir.append(empty(w))
            #流速/m/s
            oceanCurrent.append(empty(tds[11].div.string))
    except:
        restart_program()
    station_names=[station_name]*len(times)
    station_ids=[station_id]*len(times)
    # print(station_names)
   
    return dates,times,station_names,tides,waves,waveDir,wave_T,wind,windScale,windDir,gustiness,gustinessScale,seaTem,airTem,air_pre,oceanCurrent,oceanCurrentDir,station_ids
def data_urls():
    station_ids=[]
    station_names=[]
    urls=[]
    with open('station.json','r')as f:
        data=json.load(f)
    for station_id in data[0]:
        station_ids.append(station_id)
        station_names.append(data[0][station_id])
        # 48小时，实时更新数据url https://www.cwb.gov.tw/V8/C/M/OBS_Marine/48hrsSeaObs_MOD/MC4A02.html?
        # 30天资料，延时一天更新数据url https://www.cwb.gov.tw/V8/C/M/OBS_Marine/30daysSeaObs_MOD/M%s.html?
        urls.append('https://www.cwb.gov.tw/V8/C/M/OBS_Marine/48hrsSeaObs_MOD/M%s.html?'%(station_id))

    return urls,station_ids,station_names

def save_data(dates,times,station_names,tides,waves,waveDir,wave_T,wind,windScale,windDir,gustiness,gustinessScale,seaTem,airTem,air_pre,oceanCurrent,oceanCurrentDir,station_ids):
    station=station_names[0]
    # 时间拼在一起
    data_datetime=[]
    for i in range(len(times)):
        data_datetime.append(dates[i]+' '+times[i])
    conn=pymysql.connect(host='127.0.0.1',port=3306,user='root',passwd='ybt4fybt',db='tw')
    print('Good luck!数据库连接成功！')
    cursor=conn.cursor() 
    # 判断时间是否重复
    print('%s正在剔除重复数据~~~~~~~~~'%station)
    sql="SELECT 时间 FROM cwbdata WHERE (`站点名称`=%s AND`时间`>=%s AND`时间`<=%s)"
    cursor.execute(query=sql,args=[station_names[0],data_datetime[-1],data_datetime[0]])
    d_date=[]
    for d in cursor.fetchall():
        d_date.append(d[0])

    indexs=[]
    if d_date!=[]:
        for datedata in d_date:
            datedata=datedata.strftime(r"%Y/%m/%d %H:%M")
            indexs.append(data_datetime.index(datedata))
    # 删除多个相同的元素
    def del_same(a,b):
        a_index = [i for i in range(len(a))]
        a_index = set(a_index)
        b_index = set(b)
        index = list(a_index-b_index)
        a = [a[i] for i in index]
        return a
    #根据数据时间对比后，删除数据库中已存在重复的数据，只保留新数据
    data_datetime=del_same(data_datetime,indexs)
    station_names=del_same(station_names,indexs)
    tides=del_same(tides,indexs)
    waves=del_same(waves,indexs)
    waveDir=del_same(waveDir,indexs)
    wave_T=del_same(wave_T,indexs)
    wind=del_same(wind,indexs)
    windScale=del_same(windScale,indexs)
    windDir=del_same(windDir,indexs)
    gustiness=del_same(gustiness,indexs)
    gustinessScale=del_same(gustinessScale,indexs)
    seaTem=del_same(seaTem,indexs)
    airTem=del_same(airTem,indexs)
    air_pre=del_same(air_pre,indexs)
    oceanCurrent=del_same(oceanCurrent,indexs)
    oceanCurrentDir=del_same(oceanCurrentDir,indexs)
    station_ids=del_same(station_ids,indexs)
    if data_datetime!=[]:
        print('%s,发现新数据，时间为:'%station)
        print(data_datetime)
        print('%s,开始入库！~~~~~~~~~'%station)
        t1=datetime.datetime.now()
        for i in range(len(data_datetime)):
            sql="INSERT INTO cwbdata(时间,站点名称,潮高,浪高,浪向,浪周期,风速,风级,风向,阵风,阵风风级,海温,气温,气压,流速,流向,站点编号) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(query=sql,args=[data_datetime[i],station_names[i],tides[i],waves[i],waveDir[i],wave_T[i],wind[i],windScale[i],windDir[i],gustiness[i],gustinessScale[i],seaTem[i],airTem[i],air_pre[i],oceanCurrent[i],oceanCurrentDir[i],station_ids[i]])
            conn.commit()
        t2=datetime.datetime.now()
        t3=t2-t1
        print('%s数据保存完毕！共耗时%s'%(station,t3))
    else:
        print("%s,未发现新数据..........."%station)
    cursor.close()
    conn.close()
    t=random.randint(1,4)
    print('休眠%s秒\n'%t)
    time.sleep(t)

    # 重启程序函数
def restart_program():
    print('发生异常~~~~~~~~')
    sleeptime(300)
    print('重新启动python~~~~~~')
    python=sys.executable
    os.execl(python,python,*sys.argv)

def sleeptime(deltt):#参数，睡眠秒数
    t1 = datetime.datetime.now()
    delttime1 = datetime.timedelta(seconds=deltt)
    t2 = t1 + delttime1
    print("休眠%s分钟，下次启动时间为：%s "%(deltt/60,t2.strftime(r"%Y/%m/%d %H:%M:%S")))
    time.sleep(deltt)

sys.stdout = Logger("fublog.txt")#日值记录

j=1
while True:
    urls,station_ids,station_names=data_urls()
    for url,station_id,station_name in zip(urls,station_ids,station_names):
        print('正在解析第%s/%s个'%(j,len(urls)))
        dates,times,station_names,tides,waves,waveDir,wave_T,wind,windScale,windDir,gustiness,gustinessScale,seaTem,airTem,air_pre,oceanCurrent,oceanCurrentDir,station_ids=get_data(url,station_id,station_name)
        # time.sleep(random.randint(1,4))
        save_data(dates,times,station_names,tides,waves,waveDir,wave_T,wind,windScale,windDir,gustiness,gustinessScale,seaTem,airTem,air_pre,oceanCurrent,oceanCurrentDir,station_ids)
        j=j+1
    print('所有数据入库结束~~~~~~~~~~~~')
    j=1
    # 睡眠时间1800秒
    sleeptime(1800)





# # data_dict={
#             '日期':dates,
#             '时间':times,
#             '站点名称'：,
#             '潮高':tides,
#             '浪高':waves,
#             '浪向':waveDir,
#             '浪周期':wave_T,
#             '风速':wind,
#             '风级':windScale,
#             '风向':windDir,
#             '阵风':gustiness,
#             '阵风风级':gustinessScale,
#             '海温':seaTem,
#             '气温':airTem,
#             '气压':air_pre,
#             '流速':oceanCurrent,
#             '流向':oceanCurrentDir,
#             '站点编号'：,
# #             }
# df=pd.DataFrame(data_dict,index=None)
# df.to_csv('cwb.csv')
# print(times)








