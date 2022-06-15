#-*- coding:utf-8 -*-
# my_pc : z40kj740zp2w34tuql0a
# server_pc : hwbpt09z26145s5ca2cz
# pip install requests
# pip install bs4
# pip install selenium
from selenium.webdriver.common.alert import Alert
import requests, xmltodict, datetime, openpyxl, asyncio, functools
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
import datetime
from SCOPUS import SCOPUS_crawler
from WOS import WOS_crawler
from ParsingHelper import Parser
from ParsingHelper import FileUpdateHandler
from ParsingHelper import FileObserver
from selenium.common.exceptions import ElementNotVisibleException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from pyvirtualdisplay import Display
from collections import OrderedDict
from urllib.request import urlopen
from kafka import KafkaProducer
from pymongo import MongoClient
from selenium import webdriver
from bs4 import BeautifulSoup
from langdetect import detect
from threading import Thread
from random import randint
from json import dumps
from time import sleep
from multiprocessing import Pool
import logging as log
import csv
import shutil
import os.path
#import pprint
# import async
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Value, Array, Lock

logging.basicConfig()
logger = logging.getLogger('retry_count')
logger.setLevel(logging.INFO)


def __main__ ():
     #page_split = page.split(',')
     """ #1. 파라미터 파싱 """
     keyword = sys.argv[1:len(sys.argv)-4]   #// keyword = --k ~~~~ // year = --y 등 인자값 여려개일때 묶어야하나?
     keyId = int(sys.argv[len(sys.argv)-4])
     year = sys.argv[len(sys.argv)-3]
     site = sys.argv[len(sys.argv)-2]
     korea_option = int(sys.argv[len(sys.argv)-1])

     with open('/home/search/apps/product/etc/config.json') as f:
         config = json.load(f)

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year, "/ Site : ", site)
     Producer(keyword, keyId, year, site, korea_option, config).crawl()

""" #2. Producer 객체 생성 (crawler)"""
class Producer:
    # sleepNum = 1
    # producer = KafkaProducer(bootstrap_servers='203.255.92.141:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    '''
    @ Method Name		: __init__
    @ Method explain	: 클래스 생성자
    @ keyword     	  	: 입력 키워드
    @ keyId     	  	: 검색 ID
    @ year				: 검색 년도
    @ site      		: 검색 사이트
    @ korea_option      : 한국 결과 / 종합 결과 flag (WOS / SCOPUS 만 사용)
    '''

    def __init__(self, keyword, keyId, year, site, korea_option, config):
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.site = site
        self.korea_option = korea_option
        self.config = config
        self.producer = KafkaProducer(bootstrap_servers= self.config["IP"] + ':9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

        print("korea", self.korea_option)

        """ Note #1 : 10만건 이상 검색 시 stop """
        self.stopCount = 100000
        self.timeout = 60
        self.temp = {}

        """ #3. API에 사용될 입력 키워드 파싱 """
        self.keywords = {'and' : [], 'or' : [], 'not' : []}
        for k in self.keyword :
            type = 'and'
            if ('|' in k)   == True:
                # k =  k[1:len(k)-1] # and 연산
                type = 'or'
                 # ttotal = -1
            elif ('!' in k) == True: # not 연산
                 # k = k[1:]
                type = 'not'
            # else :
            #     k = '|' + k

            self.keywords[type].append(k)
        a = ""
        mainType = ""
        if len(self.keywords['and']) > 0 :
            mainType = 'and'
        else :
            mainType = 'or'

        a =  " ".join(self.keywords[mainType])
        a += " ".join(self.keywords['not'])
        self.apiKeyword = a

    '''
    @ Method Name		: flush
    @ Method explain	: Kafka 메세지를 강제로 전송
    '''
    def flush(self):
        self.producer.flush()


    """ #4. crawling 시작 """
    '''
    @ Method Name		: crawl
    @ Method explain	: 각 사이트 별 수집기 실행
    '''
    def crawl(self):
        print("crawl")
        name = self.site
        """ #5. site 별 function 실행 코드 myDict에 맵핑 되어야함 """
        f = lambda self, x : self.myDict.get(x, lambda x : self.noSite())(self)
        f(self, name)

########################################################################################### NTIS
    """ Note #2. NTIS Helper """
    '''
    @ Method Name		: ntis_remove_html_tags
    @ Method explain	: NTIS의 html 태그들을 삭제
    @ data				: 태그 삭제 전 정보
    '''
    def ntis_remove_html_tags(self, data):
        if data == 'None':
            pass
            # elif '-' in data:
            #     p = re.compile(r'-')
        else:
            p = re.compile(r'<.*?>||-')
            return p.sub('', str(data))

    """ #5. NTIS crawling """
    '''
    @ Method Name		: NTIS
    @ Method explain	: NTIS 수집기 수행
    '''
    def NTIS(self):
        # producer = KafkaProducer(bootstrap_servers='203.255.92.48:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        # self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'isPartial' : False})
        # self.producer.flush()
        # return
        self.dt = datetime.datetime.now()

        # # Connect to NTIS url using query Parameter
        url = self.config["NTIS_URL"]
        self.queryParams = '?' + 'apprvKey=' + self.config["NTIS_API_KEY"] \
            + '&collection=' + 'project' + '&SRWR=' + self.apiKeyword \
            + '&searchRnkn=' + 'DATE/DESC' + '&displayCnt=' + '100' + '&addquery=PY=' + self.year + "/MORE"
        self.url = url + self.queryParams

        # # whether or not loading page is collect
        rescode = 0
        try :
            resq = requests.get(self.url, timeout=self.timeout)
            rescode = resq.status_code
        except Exception as e :
            print(e)
            rescode = 500
        # print(rescode)
        if rescode == 200:
            conn = resq.content
            self.xmlDict = xmltodict.parse(conn)
            print(self.url)

            """ FIX #1 => API LIMIT => How to ? """

            """ Initial API LIMIT """
            nothing = {}
            if 'ERROR' in self.xmlDict['RESULT'] and int(self.xmlDict['RESULT']['ERROR']['CODE']) == 3:
                nothing['progress'] = 1.0
                nothing['keyId'] = self.keyId
                nothing['API LIMIT'] = -3
                self.producer.send(self.site, value = nothing)
                self.flush()
            else:

                self.pageNum = self.xmlDict['RESULT']['TOTALHITS']
                print("검색결과 : " + self.pageNum)

                """ #5-1. 검색 결과 없을 때 행위 """
                if int(self.pageNum) == 0:
                    nothing['progress'] = 1.0
                    nothing['keyId'] = self.keyId
                    nothing['isPartial'] = False
                    self.producer.send(self.site , value=nothing)
                    self.flush()
                    print(nothing)
                    print("검색결과가 없습니다.")
                else:
                    cnt = 0
                    """ Extract the required Data """
                    pageNumb = int(math.floor((int(self.pageNum)/100)))
                    print('PAGE NUMBER >> ', (pageNumb))
                    rawData = {}

                    START =  datetime.datetime.now()
                    for i in range(0, pageNumb+1):
                        try :
                            """ #5-2. 10만건 이상 시 stop """
                            if cnt > self.stopCount :
                                isPartial = True
                                break
                            #rawData = {}
                            rawData['qryKeyword'] = self.keyword
                            rawData['qryTime'] = "{}{}{}{}{}".format(self.dt.year, self.dt.month, self.dt.day, self.dt.hour, self.dt.minute)
                            rawData['keyId'] = self.keyId
                            add_queryParam = '&startPosition=' + '{0}'.format((i*100)+1)
                            final_url = self.url + add_queryParam
                            conn1 = requests.get(final_url, timeout=self.timeout).content
                            final_xmlDict = xmltodict.parse(conn1)
                            j = 0
                            numPerror = 0

                            if 'ERROR' in self.xmlDict['RESULT'] and int(self.xmlDict['RESULT']['ERROR']['CODE']) == 3:
                                nothing['progress'] = 1.0
                                nothing['keyId'] = self.keyId
                                nothing['API LIMIT'] = -3
                                isPartial = True
                                nothing['isPartial'] = True
                                self.producer.send(self.site, value=nothing)
                            else:
                                """ #5-4. Field 파싱 """
                                x = final_xmlDict['RESULT']['RESULTSET']['HIT']
                                if type(x) != list:
                                    x = [final_xmlDict['RESULT']['RESULTSET']['HIT']]
                                for HIT in x:
                                    j += 1
                                    cnt += 1
                                    rawData['id']        = HIT['ProjectNumber']
                                    rawData['koTitle']   = self.ntis_remove_html_tags(HIT['ProjectTitle']['Korean'])
                                    rawData['enTitle']   = self.ntis_remove_html_tags(HIT['ProjectTitle']['English'])
                                    rawData['mng']       = self.ntis_remove_html_tags(HIT['Manager']['Name'])
                                    rawData['mngId']     = HIT['Manager']['HumanID']
                                    rawData['rsc']       = self.ntis_remove_html_tags(HIT['Researchers']['Name'])
                                    rawData['rscId']     = HIT['Researchers']['HumanID']
                                    rawData['cntRscMan'] = HIT['Researchers']['ManCount']
                                    rawData['cntRscWom'] = HIT['Researchers']['WomanCount']
                                    rawData['goalAbs']   = self.ntis_remove_html_tags(HIT['Goal']['Full'])
                                    rawData['absAbs']    = self.ntis_remove_html_tags(HIT['Abstract']['Full'])
                                    rawData['effAbs']    = self.ntis_remove_html_tags(HIT['Effect']['Full'])
                                    rawData['koKeyword'] = self.ntis_remove_html_tags(HIT['Keyword']['Korean'])
                                    rawData['enKeyword'] = self.ntis_remove_html_tags(HIT['Keyword']['English'])
                                    rawData['odAgency']  = self.ntis_remove_html_tags(HIT['OrderAgency']['Name'])
                                    rawData['ldAgency']  = self.ntis_remove_html_tags(HIT['LeadAgency'])
                                    rawData['prdStart']  = HIT['ProjectPeriod']['Start']
                                    rawData['prdEnd']    = HIT['ProjectPeriod']['End']
                                    rawData['mscL']      = HIT['MinistryScienceClass']['Large']
                                    rawData['mscM']      = HIT['MinistryScienceClass']['Medium']
                                    rawData['mscS']      = HIT['MinistryScienceClass']['Small']
                                    rawData['perfAgent'] = HIT['PerformAgent']
                                    rawData['region']    = HIT['Region']
                                    rawData['totalFund'] = HIT['TotalFunds']
                                    rawData['progress']  = (cnt)/int(self.pageNum)
                                    rawData['isPartial'] = False
                                    # rawData['loop']      = cnt # check for collecting data
                                    # print((cnt)/int(pageNum))
                                    """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                                    self.producer.send(self.site, rawData)
                        except Exception as e :
                            print(e)
                            #print("parsing Error")
                            numPerror += 1
                        print(i, " page Comp.", (pageNumb-i), ' remained', ' Num Parsing Error : ', numPerror )

                    END =  datetime.datetime.now()
                    print("Elapsed Time : ", (END-START), pageNumb)
                    # sleep(randint(0,1))
                    if 'progress' in rawData and rawData['progress'] != 1:
                        self.producer.send(self.site, {"keyId" : self.keyId, 'progress' : 1, 'isPartial' : False})
                    self.flush()

        else:
            """ #5-6. 검색 실패시 """
            nothing['progress'] = 1.0
            nothing['keyId'] = self.keyId
            nothing['fail'] = 1
            self.producer.send(self.site, value=nothing)
            print("Error Code:" + rescode)

#################################################################################################################SCOPUS
    '''
    @ Method Name		: yearToStr
    @ Method explain	: Year 정보를 Scopus 전용 Query 문으로 변형
    @ year				: year String
    '''
    def yearToStr(self, year) :
        return  "(LIMIT-TO( PUBYEAR, {}))".format(year)

    '''
    @ Method Name		: removeImperativeKeyword
    @ Method explain	: Not keyword의 느낌표를 제거
    @ notKeyword		: not keyword String 값
    '''
    def removeImperativeKeyword(self, notKeyword) :
        return notKeyword[1:]

    '''
    @ Method Name		: create_query
    @ Method explain	: SCOPUS 질의 생성 함수
    @now				: 현재 년도(ex.2021)
    @maxProc			: process 수(ex.12개)
    '''
    def create_query(self, now, maxProc):
        # present = 2021
        # diff_year = (present - int(self.year)+1)//2

        s_queary = ''
        and_not = ''
        if len(self.keywords['not']) == 1:
            and_not = 'AND NOT ' + self.keywords['not'][0][1:]
        elif len(self.keywords['not']) > 1:
            notLists = map(self.removeImperativeKeyword, self.keywords['not'])
            and_not = ' AND NOT '.join(notLists)

        if len(self.keywords['and']) > 0:
            self.keywords['and'] = list(filter(lambda x : x.lower() != 'and', self.keywords['and']))
            s_query = ' AND '.join(self.keywords['and']) + ' {}'.format(and_not)
        else:
            s_query = ' OR '.join(self.keywords['or']) + ' {}'.format(and_not)

        query = 'TITLE-ABS-KEY ( {} )'.format(s_query)
        if self.korea_option == 0 :
            query += " AND  ( LIMIT-TO ( AFFILCOUNTRY ,  \"South Korea\" ) )"

        rtv = []
        lists = list(range(int(self.year), int(now)+2))
        numProc = min(len(lists), maxProc)
        perYear = len(lists) // numProc
        x = list(map(self.yearToStr, lists))

        for i in range(numProc) :
            partYear = []
            if i == numProc-1 :
                partYear = x[i*perYear:]
            else :
                partYear = x[i*perYear:i*perYear+perYear]

            rtv.append([(query+" AND {} ").format(" AND ".join(partYear)), query, "{} process".format(i)])

        print(rtv)
        return rtv, numProc

    '''
    @ Method Name		: run_crawler
    @ Method explain	: SCOPUS 수집기 작동 함수 (멀티 프로세싱)
    @ crawl_end			: crawling 종료(True: End, False: not yet)
    @ parse_end			: parsing 종료(True: End, Flase: not yet)
    @ num_data			: download file 수
    @ total_data		: 첫 검색 결과 논문 수
    @ path				: download 파일 저장 위치
    @ temp				: 각각의 process query list(temp:[query+year, query, process])
    @ lock				: lock acquire 위한 변수
    @ numProc			: process 수
    @ isPartial			: 부분 crawling 여부 저장
    @ parse_data		: parsing된 파일 수
    @ chrome_failed		: chome ERROR 체크
    '''
    def run_crawler(self, crawl_end, parse_end, num_data, total_data, path, temp, lock, numProc, isPartial, parse_data, chrome_failed):
        f_Queary = temp[0]
        Queary   = temp[1]
        p        = temp[2]
        site = SCOPUS_crawler(f_Queary, Queary, p, num_data, self.keyId, isPartial, chrome_failed, self.korea_option, self.config["SCOPUS_URL"], self.config["ETC_PATH"], self.config["PROJECT_PATH"])
        try:

            aml = len(f_Queary.replace(Queary+' AND (LIMIT-TO',''))

            start     = time.time()
            open_flag = site.open_site(f_Queary)
            if open_flag == True:
                count     = site.total_count(f_Queary)
                total_data.value += count

                if count == 0:
                    pass
                elif count <= 2000:
                    site.download(count, isPartial)

                if  (count > 2000) and (aml < 30):
                    site.crawl(f_Queary, count)

                elif (count >2000) and (aml > 30):
                    site.years()            # 년도별 논문 수 딕셔너리
                    print('years완료')
                    site.under2000_years()
                    site.over2000_years(start)

            crawl_end.value += 1

            cfile = path + "/crawler_end.txt"
            cnt = 12
            while parse_end.value == 0 :
                if  (crawl_end.value == numProc) and (num_data.value - 1 == parse_data.value or num_data.value == parse_data.value ):
                    if (num_data.value != parse_data.value) and cnt > 0:
                        time.sleep(5)
                        cnt -= 1
                    else :
                        lock.acquire()
                        if not os.path.isfile(cfile):
                            with open(cfile, 'w') as f:
                                f.write("--end--")
                                print('crawling 종료 ===> 종료 txt파일을 생성합니다. ')
                        lock.release()

                if (crawl_end.value == numProc) and (total_data.value == 0):
                    parse_end.value = 1

            site.driver.quit()
            print(">>>>>>>>>>>>>>>>>>>>", p+'프로세스 quit', "<<<<<<<<<<<<<<<<<<<<" )
        except:
            crawl_end.value += 1
            site.driver.quit()
            print(">>>>>>>>>>>>>>>>>>>>", p+'프로세스 quit', "<<<<<<<<<<<<<<<<<<<<" )
            print("crawl end")

            print("Producer 강제 종료")
            # return
        finally :
            lock.acquire()
            if crawl_end.value == numProc and os.path.isdir(path):
                if total_data.value == 0:
                    with open(cfile, 'w') as f:
                        f.write("--end--")
                print('검색 결과 0 개 ===> 종료 파일을 생성합니다. ')
                shutil.rmtree(path)
                os.remove(self.config["PROJECT_PATH"]+"/producer/Crawled_file/SCOPUS/"+str(self.keyId)+"log.log")    # 로그 파일 삭제
            lock.release()

    '''
    @ Method Name		: SCOPUS
    @ Method explain	: SCOPUS 수집기
    '''
    def SCOPUS(self):
        #self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'isPartial' : False})
        #self.producer.flush()
        # #
        #return
        os.chdir(self.config["PROJECT_PATH"]+"/producer/Crawled_file/SCOPUS")
        path = (self.config["PROJECT_PATH"]+"/producer/Crawled_file/SCOPUS/" + str(self.keyId))

        lock = Lock()
        numProc = 6
        processList = []
        crawl_end = Value('i', 0)
        parse_end = Value('i', 0)
        num_data = Value('i', 0)
        total_data = Value('i', 0)
        parse_data = Value('i', 0)
        isPartial = Value('b' , 0)
        is_not_active = Value('b' , 0)
        chrome_failed = Value('b' , 0)

        try:
            if not(os.path.isdir(path)):
                os.makedirs(os.path.join(path))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to create directory!")
                raise

        dt = datetime.datetime.now()
        start = time.time()
        list, numProc = self.create_query(dt.year, numProc)
        fileObserver = FileObserver(path, crawl_end, parse_end, parse_data, num_data, total_data, self.keyId, self.site, isPartial, len(list), is_not_active, chrome_failed, self.config["IP"])


        for l in list :
            p = Process(target=self.run_crawler, args=(crawl_end, parse_end, num_data, total_data, path, l, lock, numProc, isPartial, parse_data, chrome_failed))
            p.start()
            processList.append(p)
            time.sleep(1)

        p2 = Process(target=fileObserver.run)
        p2.start()
        processList.append(p2)


        for p in processList :
            p.join()

        print("run end")
        min = (time.time() - start) // 60
        print('수집완료',"time: ", min, '분')
        nothing ={}


        if total_data.value == 0 :
            nothing['progress'] = 1.0
            nothing['keyId'] = self.keyId
            print(nothing)

###############################################################################################################WOS
    '''
    @ Method Name     	: WOS_crawler_start
    @ Method explain  	: WOS 크롤링 동작 함수
    @ crawl_end			: crawling 종료(True: End, False: not yet)
    @ parse_end			: parsing 종료(True: End, Flase: not yet)
    @ parse_data		: parsing된 파일 수
    @ num_data			: download file 수
    @ total_data		: 첫 검색 결과 논문 수
    @ path				: download 파일 저장 위치
    @ isPartial			: 부분 crawling 여부 저장
    @ is_not_active   	: HTTP code확인(403 error) (1 = True / 0 = False) <-- 한번에 많은 데이터를 다운로드 받거나 기계적 접근이 감지 되면 차단당함
    @ chrome_failed		: chome ERROR 체크
    '''

    def WOS_crawler_start(self, crawl_end, parse_end, parse_data, num_data, total_data, path, isPartial, is_not_active, chrome_failed):
        if len(self.keyword) > 1:
            a = 'TS =' + self.keyword[0] + ' '
            for k in self.keyword[1:]:
                # if ('"' in k) == True:
                if ('!' in k) == True: # not 연산
                    k = re.sub('!','',k)
                    a += 'NOT TS = ' + k + ' '
                else:
                    _k = re.sub('"','',k) # and 연산
                    a += 'AND TS = ' + _k + ' '
                #     a += 'OR TS = ' + k + ' '      # or 연산
        else:
            a = 'TS = ' + self.keyword[0] + ' '

        now = time.localtime()
        query_keyword = a
        query_year = 'AND PY =' + str(self.year) + '-' + str(now.tm_year)
        site = WOS_crawler(query_keyword, crawl_end, parse_end, parse_data, num_data, self.keyword, self.keyId, self.year, total_data, path, isPartial, is_not_active, self.korea_option, chrome_failed, self.config["WOS_URL"], self.config["ETC_PATH"])
        site.WOS()


        crawl_end.value = 1

        try:
            while parse_end.value == 0 :
                if  crawl_end.value == 1 :
                    print("parse end wait")
                    if total_data.value == 0:
                        parse_end.value = 1
                    elif num_data.value <= parse_data.value:                    # 다운받은 파일 수 보다 파싱 수가 더 많을 때, (파싱에러)
                        file_names = os.listdir(path)
                        if "crdownload" in file_names[0]:
                            pass
                        else:
                            parse_end.value = 1
                time.sleep(3)

            if os.path.isdir(path):
                shutil.rmtree(path)
                print("crawl end====> 파일을 삭제합니다.")

            site.driver.quit()
            print("crawl end")
            return

        except:
            parse_end.value = 2
            site.driver.quit()
            print("crawl end")
            if os.path.isdir(path):
                shutil.rmtree(path)
                print("crawl end====> 파일을 삭제합니다.")
            print("Producer 강제 종료")
            return

###########################################################################################################
    '''
    @ Method Name     : WOS
    @ Method explain  : WOS 크롤링 및 ParsingHelper 프로세스 시작 함수
    '''
    def WOS(self):

        os.chdir(self.config['PROJECT_PATH']+'/producer/Crawled_file/WOS')
        path = (self.config['PROJECT_PATH']+"/producer/Crawled_file/WOS/" + str(self.keyId))
        try:
            if not(os.path.isdir(path)):
                os.makedirs(os.path.join(path))
                print('폴더 생성 : ', path)

        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to create directory!")
                raise

        processList = []
        crawl_end = Value('b', 0) # 0 ,  no , 1 yes
        parse_end = Value('b', 0)
        num_data = Value('i', 0)
        total_data = Value('i', 0)
        parse_data = Value('i', 0)
        isPartial = Value('b' , 0)
        is_not_active = Value('b' , 0)
        chrome_failed = Value('b' , 0)
        numProcess = 1

        fileObserver = FileObserver(path, crawl_end, parse_end, parse_data, num_data, total_data, self.keyId, self.site, isPartial, numProcess, is_not_active, chrome_failed, self.config["IP"])
        p = Process(target=self.WOS_crawler_start, args=(crawl_end, parse_end, parse_data, num_data, total_data, path, isPartial, is_not_active, chrome_failed))
        p.start()

        p2 = Process(target=fileObserver.run)
        p2.start()

        p.join()
        p2.join()
#################################################################################################################DBPIA

    """ #5. DBPIA crawling"""
    '''
    @ Method Name     : DBPIA
    @ Method explain  : DBPIA 크롤링
    '''
    """ #5. DBPIA crawling"""
    def DBPIA(self):

        a = " "

        keywords = {'and' : [], 'or' : [], 'not' : []}

        dt = datetime.datetime.now()
        url = self.config["DBPIA_URL"]
        queryParams = '?' + 'key=' + self.config["DBPIA_API_KEY"] + '&target='+'se_adv' + '&searchall=' + self.apiKeyword  +'&pagecount='+'100' + '&pyear='+'3'+ '&pyear_start=' +self.year +'&pyear_end='+ str(dt.year)  +'&sorttpye=' +'1'
        url = url + queryParams

        rescode = 0
        try :
            resq = requests.get(url, timeout = self.timeout)
            rescode = resq.status_code
        except Exception as e :
            print(e)
            rescode = 500

        isPartial = False

        print(url)
        if rescode ==200:
            conn = resq.content
            xmldict = xmltodict.parse(conn)
            nothing = {}
            if 'error' in xmldict :
                nothing['progress'] = 1.0
                nothing['keyId'] = self.keyId
                nothing['isPartial'] = False
                self.producer.send(self.site, value=nothing)
                self.flush()
            else:
                total_count=int(xmldict['root']['paramdata']['totalcount'])
                print(total_count)
                pagenum = int(math.ceil((int(total_count)/100)))

                hundred = 100
                # total_count += 1000# for progress
                temp_total_count =int(total_count)
                temp_count = 0
                temp_rawData = {}
                success_count = 0
                fail_count = 0

                """ #5-1. 검색 결과 없을 떄 행위 """
                if int(pagenum)==0:
                    nothing['progress'] = 1.0
                    nothing['keyId'] = self.keyId
                    nothing['isPartial'] = False
                    self.producer.send(self.site, value=nothing)
                    self.flush()
                    print(nothing)
                    print("검색결과가 없습니다.")
                else:
                    cnt = 0
                    pagenum = int(math.ceil((int(total_count)/hundred)))
                    # try :
                    START =  datetime.datetime.now()
                    for i in range(1,pagenum+1):
                        try:

                            """ #5-2. 10만건 이상 시 stop """
                            if cnt > self.stopCount :
                                isPartial = True
                                break
                            add_queryparam = '&pagenumber=' +'{}'.format(i)
                            final_url = url + add_queryparam


                            """ #5-3. API 응답 정상이 아닐 때 n번 재요청 """
                            retryCount = 5
                            isOK = False
                            while not isOK :
                                try :
                                    if i != 1:
                                        restemp = requests.get(final_url, timeout = self.timeout)
                                        conn = restemp.content

                                    final_xmldict = xmltodict.parse(conn)
                                    page_count = min(temp_total_count,hundred)

                                    isOK = True
                                    print(i, "isOk")
                                except Exception as e :
                                    print(e)
                                    print("retry count = ", retryCount)
    #                                sleep(randint(5,10))


                                    retryCount -= 1
                                if retryCount == 0 :
                                    break

                            if retryCount == 0 :
                                isPartial = True
                                print("page error")
                                continue

                            numPerror = 0

                            """ #5-4. Field 파싱 """
                            for j in range(0, page_count):
                                cnt = cnt + 1
                                # prin
                                rawData = {}
                                rawData ['qryKeyword'] = self.keyword
                                rawData ['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                                rawData ['keyId'] = self.keyId
                                rawData['citation'] = 0
                                try :
                                    if page_count == 1:
                                        item = final_xmldict['root']['result']['items']['item']
                                    else:
                                        item = final_xmldict['root']['result']['items']['item'][j]
                                    # print(item)
                                    # print(item['authors']['author'])
                                    if isinstance(item['authors']['author'], list):
                                        # print("in author")
                                        name_list = []
                                        id_list = []
                                        name_list = [0]*len(item['authors']['author'])
                                        id_list = [0]*len(item['authors']['author'])

                                        for z in range (0, len(item['authors']['author'])):
                                            name_list[z]=item['authors']['author'][z]['name']
                                            id_list[z]=','.join(re.findall('\d+',item['authors']['author'][z]['url']))

                                        rawData['author']=";".join(name_list)
                                        rawData['author_id']=';'.join(id_list)
                                        # print(name_list)
                                        # print(id_list)
                                    else:
                                       # rawData['author'] = {'author_name' : item['authors']['author']['name'],'author_id':re.findall('\d+',item['authors']['author']['url'])

                                       # print("out author")
                                       rawData['author'] = item['authors']['author']['name']
                                       rawData['author_id'] =",".join(re.findall('\d+',item['authors']['author']['url']))
                                    # print(rawData)
                                    rawData['title'] = re.sub('<.+?>','',item['title'])
                                   # rawData['author'] = item['authors']['author']
                                    if "name" in  item['publication']:
                                        rawData['journal'] = item['publication']['name']
                                    else:
                                        rawData['journal'] = item['publisher']['name']

                                    rawData['issue_inst'] = item['publisher']['name']

                                    if "pages" in item:
                                        page_search = re.findall('\d+',item['pages'])
                                        rawData['start_page'] = page_search[0]
                                        rawData['end_page'] = page_search[1]
                                    else:
                                        rawData['start_page'] = 0
                                        rawData['end_page'] = 0


                                    rawData['keyId'] = self.keyId
                                    rawData['id'] = 'NODE' + item['link_url'].split('NODE')[1]

                                    if item['dreg_name'] is None:
                                        rawData['dreg_name']=None
                                    elif 'KCI' in item['dreg_name']:
                                        rawData['dreg_name']='KCI'
                                    else:
                                        rawData['dreg_name']=item['dreg_name']
                                    rawData['issue_year'] = item['issue']['yymm']
                                    rawData['english_title'] = None
                                    rawData['issue_lang'] = None
                                    rawData['paper_keyword'] = None
                                    rawData['abstract'] = None
                                    rawData['english_abstract'] = None
                                    rawData['author_inst'] = None
                                    tPro = (cnt)/int(total_count)
                                    if tPro >= 1.0 :
                                        tPro = 0.99
                                    rawData['progress'] = tPro

                                    title = re.sub('<.+?>','',item['title'])
                                    # print(title)
                                    title_sub = re.sub(r'[^가-힣]+','',title)
                                    if len(title_sub) == 0:
                                        rawData['issue_lang'] = 'eng'
                                    else:
                                        rawData['issue_lang'] = 'kor'

                                    """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                                    if temp_rawData is not None:
                                        self.producer.send(self.site , value=temp_rawData)
                                    #self.producer.send(self.site, value= temp_rawData)
                                    temp_rawData = rawData.copy()
                                except Exception as e:
                                    #print(e)
                                    #print("parsing Error")
                                    numPerror += 1
                            print(i,"번쩨 페이지 수집완료, ", (pagenum-i), ' remained', ' Num Parsing Error : ', numPerror)
                            temp_total_count = temp_total_count - 100
    #                        sleep(randint(1,3))
                        except Exception as e:
                            print(e)
                            print(i,'page error')

                    END =  datetime.datetime.now()
                    print("Elapsed Time : ", (END-START), pagenum)
                    """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                    if temp_rawData is not None:
                        temp_rawData['progress'] = 1.0
                        temp_rawData['qryKeyword'] = self.keyword
                        temp_rawData ['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                        temp_rawData ['keyId'] = self.keyId
                        temp_rawData['isPartial'] = isPartial

                        self.producer.send(self.site, value=temp_rawData)
                    else:
                        nothing['progress'] = 1.0
                        nothing['keyId'] = self.keyId
                        nothing['isPartial'] = False
                        self.producer.send(self.site, value=nothing)
                    self.flush()

        else:
            """ #5-6. 검색 실패시 """
            nothing={}
            nothing['progress'] = 1.0
            nothing['keyId'] = self.keyId
            nothing['isPartial'] = False
            self.producer.send(self.site, value=nothing)
            self.flush()
            print("Error Code" + rescode)

    """ Note #3. NDSL Helper """
    '''
    @ Method Name     	: dividePage
    @ Method explain  	: NDSL 논문 page 값 분할
    @ s 				: page string 값
    '''
    """ Note #3. NDSL Helper """
    def dividePage(self, s):
        idx = int
        if s == []:
            start_page = ''
            end_page = ''
        else:
            for i in range(len(s)):
                if s[i] == '-':
                    idx = i
                    start_page = s[3:idx]
                    end_page = s[idx+1:]
                    break
                else :#s[i] != '-':
                    start_page = s[3:]
                    end_page = ''

        return {'start_page':start_page, 'end_page':end_page}


################################################################################################NDSL(SCIENCEON)
    """ #5. NDSL crawling"""
    '''
    NDSL 코드 백업용
    '''
    def NDSL2(self):

        # self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'isPartial' : True})
        # self.producer.flush()
        # return

        dt = datetime.datetime.now()
        url = "http://openapi.ndsl.kr/itemsearch.do?keyValue=02570602&target=NART&searchField=BI&displayCount=100"
        query_params = "&startPosition=1"
        keyword_params = "&sortby=pubyear&returnType=json&responseGroup=simple&query=" + self.apiKeyword +"&callback=callback"
        url = url + keyword_params
        sss = url + query_params
        print(sss)

        rescode = 0
        try :
            resq = requests.get(url, timeout = self.timeout)
            rescode = resq.status_code
        except Exception as e :
            print(e)
            rescode = 500

        isPartial = False
        if rescode == 200:
            res = resq.text
            resSlicing = res[9:-1]
            #print(resSlicing)
            resToDict = json.loads(resSlicing)
            totalCount = resToDict['resultSummary']['totalCount']
            print("검색결과 : " + totalCount)

            nothing = {}
            temp_total_count = int(totalCount)
            temp_count    = 0
            temp_rawData  = None
            success_count = 0
            fail_count    = 0

            """ #5-1. 검색 결과 없을 떄 행위 """
            #if int(totalCount) == 0:
            #    nothing['progress'] = 1.0
            #    nothing['keyId'] = self.keyId
            #    nothing['isPartial'] = False
            #    self.producer.send(self.site, value=nothing)
            #    self.flush()
            #    print(nothing)
            #    print("검색결과가 없습니다.")
            if int(totalCount) == 0:
                # print("here")
                retryCount = 5
                while int(totalCount) == 0 :
                    try:
                        resq       = requests.get(url, timeout = self.timeout)
                        res        = resq.text
                        resSlicing = res[9:-1]
                        resToDict  = json.loads(resSlicing)
                        totalCount = int(resToDict['resultSummary']['totalCount'])
                        print("검색결과 : " + totalCount)
                    except Exception as e:
                        print(e)
                        print("retry count = ", retryCount)
                        sleep(randint(5,10))
                        retryCount -= 1
                    if retryCount == 0:
                        break

                if retryCount == 0 :
                    nothing['progress'] = 1.0
                    nothing['keyId'] = self.keyId
                    nothing['isPartial'] = False
                    self.producer.send(self.site, value=nothing)
                    self.flush()
                    print(nothing)
                    print("검색결과가 없습니다.")

            else:
                cnt = 0
                pageNumb = int(math.floor((int(totalCount)/100)))

                total = (pageNumb+1) * 100 + 100
                print('PAGE NUMBER >> ', (pageNumb))
                try:
                    START =  datetime.datetime.now()
                    for i in range(pageNumb+1):
                        """ #5-2. 10만건 이상 시 stop """
                        # temprally setting value
                        # if cnt > self.stopCount :
                        if cnt > 10000 :
                            isPartial = True
                            break
                        j = 0
                        rawData = {}
                        rawData['qryKeyword'] = self.keyword
                        rawData['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                        rawData ['keyId'] = self.keyId
                        page_count = min(temp_total_count, 100)


                        """ #5-3. API 응답 정상이 아닐 때 n번 재요청 """
                        retryCount = 5
                        isOK = False
                        while not isOK :
                            try :
                                add_queryParam = '&startPosition=' + '{0}'.format((i*100)+1)
                                final_url = url + add_queryParam
                                print(final_url)
                                restemp = requests.get(final_url, timeout = self.timeout)
                                res = restemp.text
                                resSlicing = res[9:-1]
                                resToDict = json.loads(resSlicing)
                                isOK = True
                                print(i, "isOk")
                            except Exception as e :
                                print(e)
                                print("retry count = ", retryCount)
                                sleep(randint(5,10))

                                retryCount -= 1
                            if retryCount == 0 :
                                break

                        if retryCount == 0 :
                            isPartial = True
                            print("page error")
                            continue

                        numPerror = 0
                        if len(resToDict['outputData']) == 0:
                            isPartial = True
                            continue

                        """ #5-4. Field 파싱 """
                        for data in resToDict['outputData']:
                            # print(data)
                            j += 1
                            cnt += 1
                            try :
                                rawData['loop'] = cnt
                                rawData['issue_inst'] = data['journalInfo']['publisher']
                                rawData['journal'] = data['journalInfo']['journalTitleInfo'][0]
                                rawData['issue_year'] = data['journalInfo']['year']
                                rawData['id'] = data['articleInfo']['@kistiID']
                                rawData['title'] = self.ntis_remove_html_tags(data['articleInfo']['articleTitleInfo']['articleTitle'])
                                rawData['english_title'] = self.ntis_remove_html_tags(data['articleInfo']['articleTitleInfo']['articleTitle2'])
                                rawData['abstract'] = self.ntis_remove_html_tags(data['articleInfo']['abstractInfo'][0])
                                rawData['english_abstract'] = ''
                                rawData['citation'] = 0
                                names = []
                                insts = []
                                if isinstance(data['articleInfo']['authorInfo']['author'], str) :
                                    if len(data['articleInfo']['authorInfo']['author']) == 0 :
                                        raise Exception('not have author name')
                                    names.append(data['articleInfo']['authorInfo']['author'])
                                else :
                                    for author in data['articleInfo']['authorInfo']['author']  :
                                        if '#text' in author :
                                            names.append(author['#text'])

                                        # names.append(author['#text'])
                                p = re.compile("[A-Z]{2}[0-9]{7}")
                                if isinstance(data['articleInfo']['authorInfo']['affiliation'], str) :
                                    if len(data['articleInfo']['authorInfo']['affiliation']) == 0 :
                                        raise Exception('not have affiliation name')
                                    if re.search("[A-Z]{2}[0-9]{7}", data['articleInfo']['authorInfo']['affiliation']) != None:
                                        insts.append(self.ntis_remove_html_tags(data['articleInfo']['authorInfo']['affiliation']))
                                else :
                                    for inst in data['articleInfo']['authorInfo']['affiliation']  :
                                        if '#text' in inst :
                                            if re.search("[A-Z]{2}[0-9]{7}", inst['#text']) != None:
                                                insts.append(self.ntis_remove_html_tags(p.sub('',inst['#text'])))
                                            else:
                                                insts.append(self.ntis_remove_html_tags(inst['#text']))

                                if len(names) != len(insts) or (len(names) == 0 and len(insts) ==0):
                                    raise Exception('not matched correctly name and inst')

                                rawData['author'] = ";".join(names)
                                rawData['author_inst'] = ";".join(insts)

                                pages = self.dividePage(data['articleInfo']['page'])
                                rawData['start_page'] = pages['start_page']
                                rawData['end_page'] = pages['end_page']
                                rawData['paper_keyword'] = data['articleInfo']['keyword']

                                if self.isEnglish(rawData['title']) == 0 :
                                    rawData['issue_lang'] = 'kor'
                                else :
                                    rawData['issue_lang'] = 'eng'
                                tPro = cnt / total
                                if tPro >= 1.0 :
                                    tPro = 0.99
                                rawData['progress'] = tPro
                                # rawData['progress'] = cnt / total
                                # self.producer.send(self.site, rawData)


                                if temp_rawData is not None:
                                    self.producer.send(self.site, temp_rawData)

                                temp_rawData = rawData.copy()
                            except Exception as e :
                                #print(e)
                                # print(data)
                                #print('Parsing Error')
                                numPerror += 1
                        temp_total_count = temp_total_count - 100
                        print(i, ' page Completed', (pageNumb-i) , ' remained', ' Parsing Error : ', numPerror)
                        sleep(randint(1,3))
                    END =  datetime.datetime.now()
                    print("Elapsed Time : ", (END-START), pageNumb)
                    """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                    if temp_rawData is not None: # completed
                        temp_rawData['progress'] = 1.0
                        temp_rawData['qryKeyword'] = self.keyword
                        temp_rawData['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                        temp_rawData['keyId'] = self.keyId
                        temp_rawData['isPartial'] = isPartial
                        self.producer.send(self.site, temp_rawData)
                    else:
                        nothing['progress'] = 1.0
                        nothing['keyId'] = self.keyId
                        nothing['isPartial'] = isPartial
                        self.producer.send(self.site, nothing)
                    self.flush()

                except Exception as e:
                    """ #5-6. 검색 실패시 """
                    nothing['progress'] = 1.0
                    nothing['keyId'] = self.keyId
                    nothing['fail'] = 1
                    self.producer.send(self.site, nothing)
                    self.flush()
                    print(e)
                    print("Die")

        else:
            """ #5-6. 검색 실패시 """
            nothing['progress'] = 1.0
            nothing['keyId'] = self.keyId
            nothing['fail'] = 1
            self.producer.send(self.site, nothing)
            self.flush()
            print("Error code" + rescode)

    '''
    @ Method Name     	: NDSL
    @ Method explain  	: NDSL Crawling 실행
    '''
    def NDSL(self):
        dt = datetime.datetime.now()

        # 발행년도 Filter code #
        url = self.config["SCIENCEON_URL"] + "keyValue=" + self.config["SCIENCEON_API_KEY"] +"&target=NART&searchField=&displayCount=100"

        for y in range(int(self.year), int(dt.year)+1) :
            semi_url = url + "&sortby=pubyear&returnType=json&responseGroup=simple&query=(BI:" + self.apiKeyword +") and (PY:"+ str(y) + ")&callback=callback"
            # print(final_url + "\n")
            query_params = "&startPosition=1"
            sss = semi_url + query_params
            print(sss)

            rescode = 0
            try :
                resq = requests.get(semi_url, timeout = self.timeout)
                rescode = resq.status_code
            except Exception as e :
                print(e)
                rescode = 500

            isPartial = False
            if rescode == 200:
                res = resq.text
                resSlicing = res[9:-1]
                #print(resSlicing)
                resToDict = json.loads(resSlicing)
                totalCount = resToDict['resultSummary']['totalCount']
                print(str(y) + "년도 검색결과 : " + totalCount)

                nothing = {}
                temp_total_count = int(totalCount)
                temp_count    = 0
                temp_rawData  = None
                success_count = 0
                fail_count    = 0
                pageNumb = int(math.floor((int(totalCount)/100)))

                """ #5-1. 검색 결과 없을 때 행위 """

                if int(totalCount) == 0:
                    retryCount = 1
                    while int(totalCount) == 0 :
                        try:
                            resq       = requests.get(semi_url, timeout = self.timeout)
                            res        = resq.text
                            resSlicing = res[9:-1]
                            resToDict  = json.loads(resSlicing)
                            totalCount = int(resToDict['resultSummary']['totalCount'])
                            print("검색결과 : " + totalCount)
                        except Exception as e:
                            print(e)
                            print("retry count = ", retryCount)
                            sleep(randint(5,10))
                            retryCount -= 1
                        if retryCount == 0:
                            break

                    if retryCount == 0 :
                        if int(y) == int(dt.year):
                            nothing['progress'] = 1.0
                            nothing['keyId'] = self.keyId
                            nothing['isPartial'] = False
                            self.producer.send(self.site, value=nothing)
                            self.flush()
                            print(nothing)
                            print("검색결과가 없습니다.")
                        else:
                            continue

                else:
                    cnt = 0
                    pageNumb = int(math.floor((int(totalCount)/100)))
                    total = (pageNumb+1) * 100 + 100
                    print('PAGE NUMBER >> ', (pageNumb))
                    try:
                        for i in range(pageNumb+1):
                            """ #5-2. 1만건 이상 시 stop """
                            if cnt > self.stopCount : # self.stopCount --> 10,000 으로 수정 필요
                                isPartial = True
                                break
                            j = 0
                            rawData = {}
                            rawData['qryKeyword'] = self.keyword
                            rawData['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                            rawData['keyId'] = self.keyId
                            page_count = min(temp_total_count, 100)

                            """ #5-3. API 응답 정상이 아닐 때 n번 재요청 """
                            retryCount = 5
                            isOK = False
                            while not isOK :
                                try :
                                    add_queryParam = '&startPosition=' + '{0}'.format((i*100)+1)
                                    final_url = semi_url + add_queryParam
                                    print(final_url)
                                    restemp = requests.get(final_url, timeout = self.timeout)
                                    res = restemp.text
                                    resSlicing = res[9:-1]
                                    resToDict = json.loads(resSlicing)
                                    isOK = True
                                    print(i, "isOk")
                                except Exception as e :
                                    print(e)
                                    print("retry count = ", retryCount)
                                    sleep(randint(5,10))
                                    retryCount -= 1
                                if retryCount == 0 :
                                    break

                            if retryCount == 0 :
                                isPartial = True
                                print("page error")
                                continue

                            numPerror = 0
                            if len(resToDict['outputData']) == 0:
                                isPartial = True
                                continue

                            """ #5-4. Field 파싱 """
                            for data in resToDict['outputData']:
                                # print(data)
                                j += 1
                                cnt += 1
                                try :
                                    rawData['loop'] = cnt
                                    rawData['issue_inst'] = data['journalInfo']['publisher']
                                    rawData['journal'] = data['journalInfo']['journalTitleInfo'][0]
                                    rawData['issue_year'] = data['journalInfo']['year']
                                    rawData['id'] = data['articleInfo']['@kistiID']
                                    rawData['title'] = self.ntis_remove_html_tags(data['articleInfo']['articleTitleInfo']['articleTitle'])
                                    rawData['english_title'] = self.ntis_remove_html_tags(data['articleInfo']['articleTitleInfo']['articleTitle2'])
                                    rawData['abstract'] = self.ntis_remove_html_tags(data['articleInfo']['abstractInfo'][0])
                                    rawData['english_abstract'] = ''
                                    rawData['citation'] = 0
                                    names = []
                                    insts = []
                                    if isinstance(data['articleInfo']['authorInfo']['author'], str) :
                                        if len(data['articleInfo']['authorInfo']['author']) == 0 :
                                            raise Exception('not have author name')
                                        names.append(data['articleInfo']['authorInfo']['author'])
                                    else :
                                        for author in data['articleInfo']['authorInfo']['author']  :
                                            if '#text' in author :
                                                names.append(author['#text'])

                                            # names.append(author['#text'])
                                    p = re.compile("[A-Z]{2}[0-9]{7}")
                                    if isinstance(data['articleInfo']['authorInfo']['affiliation'], str) :
                                        if len(data['articleInfo']['authorInfo']['affiliation']) == 0 :
                                            raise Exception('not have affiliation name')
                                        if re.search("[A-Z]{2}[0-9]{7}", data['articleInfo']['authorInfo']['affiliation']) != None:
                                            insts.append(self.ntis_remove_html_tags(data['articleInfo']['authorInfo']['affiliation']))
                                    else :
                                        for inst in data['articleInfo']['authorInfo']['affiliation']  :
                                            if '#text' in inst :
                                                if re.search("[A-Z]{2}[0-9]{7}", inst['#text']) != None:
                                                    insts.append(self.ntis_remove_html_tags(p.sub('',inst['#text'])))
                                                else:
                                                    insts.append(self.ntis_remove_html_tags(inst['#text']))

                                    if len(names) != len(insts) or (len(names) == 0 and len(insts) ==0):
                                        raise Exception('not matched correctly name and inst')

                                    rawData['author'] = ";".join(names)
                                    rawData['author_inst'] = ";".join(insts)

                                    pages = self.dividePage(data['articleInfo']['page'])
                                    rawData['start_page'] = pages['start_page']
                                    rawData['end_page'] = pages['end_page']
                                    rawData['paper_keyword'] = data['articleInfo']['keyword']

                                    if self.isEnglish(rawData['title']) == 0 :
                                        rawData['issue_lang'] = 'kor'
                                    else :
                                        rawData['issue_lang'] = 'eng'

                                    a = int(y) - int(self.year)
                                    b = int(dt.year) - int(self.year) + 1

                                    # tPro = (cnt / total)
                                    tPro = a/b + (cnt / total) / b
                                    #print(tPro)

                                    # tPro = (cnt / total)   # (y - int(self.year))/(int(dt.year)-int(self.year))
                                    # tPro = (int(y) - int(self.year)) / (int(dt.year) - int(self.year) + 1) + (tPro) / (int(dt.year) - int(self.year) + 1)
                                    # print((int(y) - int(self.year)) / (int(dt.year) - int(self.year) + 1) + (tPro) / (int(dt.year) - int(self.year) + 1))

                                    if tPro >= 1.0 :
                                        tPro = 0.99
                                    rawData['progress'] = tPro
                                    # rawData['progress'] = cnt / total
                                    self.producer.send(self.site, rawData)


                                    if temp_rawData is not None:
                                        self.producer.send(self.site, temp_rawData)


                                    temp_rawData = rawData.copy()
                                except Exception as e :
                                    # print(e)
                                    # print(data)
                                    #print('Parsing Error')
                                    numPerror += 1
                            temp_total_count = temp_total_count - 100
                            print(i, ' page Completed', (pageNumb-i) , ' remained', ' Parssing Error : ', numPerror)
                            sleep(randint(1,3))

                        """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                        if int(y) == int(dt.year) :
                            if temp_rawData is not None: # completed
                                temp_rawData['progress'] = 1.0
                                temp_rawData['qryKeyword'] = self.keyword
                                temp_rawData['qryTime'] = "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute)
                                temp_rawData['keyId'] = self.keyId
                                temp_rawData['isPartial'] = isPartial
                                self.producer.send(self.site, temp_rawData)
                            else:
                                nothing['progress'] = 1.0
                                nothing['keyId'] = self.keyId
                                nothing['isPartial'] = isPartial
                                self.producer.send(self.site, nothing)
                        self.flush()

                    except Exception as e:
                        """ #5-6. 검색 실패시 """
                        if int(y) == int(dt.year):
                            nothing['progress'] = 1.0
                            nothing['keyId'] = self.keyId
                            nothing['fail'] = 1
                            self.producer.send(self.site, nothing)
                            self.flush()
                            print(e)
                            print("Die")
                        else:
                            continue

            else:
                """ #5-6. 검색 실패시 """
                if int(y) == int(dt.year) :
                    nothing['progress'] = 1.0
                    nothing['keyId'] = self.keyId
                    nothing['fail'] = 1
                    self.producer.send(self.site, nothing)
                    self.flush()
                    print("Error code" + rescode)
                else:
                    continue

############################################################################ KCI

    """ #5. KCI crawling"""
    '''
    @ Method Name     	: KCI
    @ Method explain  	: KCI Crawling 실행
    '''
    def KCI(self):

        now   = 0
        ttotal = 0

        keywords = {'and' : [], 'or' : [], 'not' : []}
        for k in self.keyword:
            type = 'and'
            if ('|' in k)   == True:
                k =  k[1:] # and 연산
                type = 'or'
            elif ('!' in k) == True: # not 연산
                k = k[1:]
                type = 'not'
            else :
                ttotal = -1
                k = "\""+k+"\""

            keywords[type].append(k)

        rkeywrods  = keywords['and']
        if ttotal == 0 :
            ttotal = len(keywords['or'])*3
            rkeywrods  = ["\""+key+"\"" for key in keywords['or']]
        elif len(keywords['and']) == 1:
            ttotal = 3

        self.loopKCI(rkeywrods, keywords['not'], now,  ttotal)

    """ Note #4. KCI Helper 1 operation handler """
    '''
    @ Method Name     	: loopKCI
    @ Method explain  	: KCI Crawling 내부 실행 코드 (요약, 제목, 키워드) 3번 수집 수행
    @ keywords 			: 키워드 목록
    @ notKeywords 		: not 키워드 목록
    @ now 				: 현재 수집된 개수
    @ total 			: 총 수집된 개수
    '''
    def loopKCI(self, keywords, notKeywords, now, total):
        isLast = False
        minNum = sys.maxsize
        minKey = ""
        andKeywords = []

        if total == -1 : # and
            print("get min keyword to process and keywords")
            numPapers = {}
            checkSearch = ['abs', 'title', 'keyword']
            for keyword in keywords :
                numPapers[keyword] = 0
                for ch in checkSearch :
                    reqUrl = self.makeKCIURL(keyword, ch)
                    print(reqUrl)

                    rescode = 0
                    try :
                        resp = requests.get(reqUrl, timeout=self.timeout, verify=False)
                        rescode = resp.status_code
                    except Exception as e :
                        print(e)
                        rescode = 500
                    if rescode == 200:
                        conn       = resp.content
                        xmldict    = xmltodict.parse(conn)
                        if 'total' in xmldict['MetaData']['outputData']['result'] :
                            numPapers[keyword] += int(xmldict['MetaData']['outputData']['result']['total'])
                        # totalCount = int(xmldict['MetaData']['outputData']['result']['total'])
                        # print(keyword, " toal = ", totalCount)
                    # if minNum > totalCount :
                    #     minNum = totalCount
                    #     minKey = keyword
                    sleep(randint(1,3))

            minKey = min(numPapers.keys(), key=(lambda k : numPapers[k]))

            keywords.remove(minKey)
            andKeywords = [key[1:len(key)-1] for key in keywords]
            # andKeywords = keywords[:]
            keywords = [minKey]
            total = 3
        # print(andKeywords)
        START =  datetime.datetime.now()
        for idx in range(len(keywords)) :

            (now, total) = self.KCIHelper(keywords[idx], notKeywords, now, total, isLast, andKeywords, 'title')
            print(keywords[idx], "title")
            (now, total) = self.KCIHelper(keywords[idx], notKeywords, now, total, isLast, andKeywords, 'keyword')
            print(keywords[idx], "keyword")
            if idx == len(keywords) -1 :
                isLast = True
            (now, total) = self.KCIHelper(keywords[idx], notKeywords, now, total, isLast, andKeywords, 'abs')
            print(keywords[idx], "abs")
        END =  datetime.datetime.now()
        print("Elapsed Time : ", (END-START), total)

    """ Note #4. KCI Helper 2 eng checker """
    '''
    @ Method Name     	: isEnglish
    @ Method explain  	: 영/한 논문 구분 함수
    @ s 				: 입력 String
    '''
    def isEnglish(self, s):
        lang = detect(s)
        if lang != 'ko' :
            return 1
        else :
            return 0

    """ Note #4. KCI Helper 3 url maker"""
    '''
    @ Method Name     	: makeKCIURL
    @ Method explain  	: KCI 검색 URL 생성
    @ keyword 			: 입력 키워드
    @ searchField 		: 제목/요약/키워드 구분
    '''
    def makeKCIURL(self, keyword, searchField) :
        dt                      = datetime.datetime.now()
        url                     = self.config["KCI_URL"]
        queryParams             = {'apiCode' : 'articleSearch', 'key' : self.config["KCI_API_KEY"], 'displayCount' : '100'}

        queryParams[searchField] = keyword
        queryParams['dateFrom'] = str(self.year)+'01'
        queryParams['dateTo']   = str(dt.year)+'12'
        page                    = '1'
        reqUrl                  = url

        for key in queryParams :
            reqUrl += "&" + key + "="+ queryParams[key]

        return reqUrl

    """ Note #4. KCI Helper 4 => crawler """
    '''
    @ Method Name     	: KCIHelper
    @ Method explain  	: KCI 검색 요청 및 Parsing
    @ keyword 			: 입력 키워드
    @ notKeywords 		: not 키워드
    @ now 				: 현재 수집된 개수
    @ total 			: 총 수집된 개수
    @ isLast 			: 마지막 요청 / 페이지 인지 확인 하기 위한 변수
    @ andKeywords 		: AND 키워드 목록
    @ searchField 		: 제목/요약/키워드 구분
    '''
    def KCIHelper(self, keyword, notKeywords,  now, total, isLast, andKeywords, searchField):
        dt                      = datetime.datetime.now()
        reqUrl = self.makeKCIURL(keyword, searchField)

        rescode = 0
        isPartial = False
        try :
            resp = requests.get(reqUrl, timeout=self.timeout, verify=False)
            rescode = resp.status_code
        except Exception as e :
            isPartial = True
            print(e,"here")
        # print(reqUrl)

        # print(resp.status_code)
        try :
            if rescode == 200:
                conn       = resp.content
                xmldict    = xmltodict.parse(conn)
                totalCount = 0
                if 'total' in xmldict['MetaData']['outputData']['result'] :
                    totalCount = int(xmldict['MetaData']['outputData']['result']['total'])
                # print(totalCount)
                total     += totalCount - 1
                numPage    = int(math.ceil((totalCount/100)))
                tempTotal  = totalCount
                nothing    = {}
                rawData    = {'qryTime' : "{}{}{}{}{}".format(dt.year, dt.month, dt.day, dt.hour, dt.minute), 'keyId' : self.keyId, 'progress' : 1, 'paper_keyword' : '', 'issue_lang' : 'kor', 'isPartial' : False}


                """ #5-1. 검색 결과 없을 떄 행위 """
                if numPage == 0:
                    print("검색결과가 없습니다.", keyword)

                tempRaw       = {}
                fieldsJournal = {'journal' : 'journal-name' ,'issue_inst' : 'publisher-name' ,'issue_year' : 'pub-year'}
                fieldsArt     = {'id' : '@article-id', 'citation' : 'citation-count' , 'start_page' : 'fpage', 'end_page': 'lpage' }
                titleArr      = ['title', 'english_title']
                absDict       = {'original' : 'abstract', 'english' : 'english_abstract', 'foreign' : 'english_abstract'}
                for tl in titleArr :
                    rawData[tl] = ''
                for abs in absDict :
                    rawData[absDict[abs]] = ''
                # acount = 0

                for p in range(1, numPage+1):
                    """ #5-2. 10만건 이상 시 stop """
                    if now > self.stopCount :
                        isPartial = True
                        break

                    """ #5-3. API 응답 정상이 아닐 때 n번 재요청 """
                    if p != 1:
                        retryCount = 5
                        isOK = False
                        while not isOK :
                            try :
                                resp = requests.get(reqUrl+'&page='+str(p), timeout = self.timeout, verify=False)
                                if resp.status_code == 200 :
                                    conn = resp.content
                                    xmldict = xmltodict.parse(conn)
                                    isOK = True
                                else :
                                    retryCount -= 1
                                    print("retry count = ", retryCount)

                                if retryCount == 0 :
                                    break
                            except Exception as e :
                                print(e)
                                retryCount -= 1
                                print("retry count = ", retryCount)

                        if retryCount == 0 :
                            isPartial = True
                            print("page error")
                            continue

                    numPerror = 0
                    for record in xmldict['MetaData']['outputData']['record'] :
                        # print(record)
                        now += 1

                        #print("try 전까지는 실행")

                        """ #5-4. Field 파싱 """
                        try :
                            journalGroup = record['journalInfo']
                            articleInfo  = record['articleInfo']
                            titles       = articleInfo['title-group']['article-title']
                            absGroup     = articleInfo['abstract-group']['abstract']
                            authorGroup  = articleInfo['author-group']['author']
                            # print('title check')
                            tempAnd = andKeywords[:]


                            for tindex in range(0, len(titles)):
                                for nkey in notKeywords :
                                    if nkey in titles[tindex]['#text'] :
                                        raise Exception('not checked', titles[tindex]['#text'])
                                for idx in range(len(tempAnd)-1,-1,-1) :
                                    comp = tempAnd[idx]
                                    ori = titles[tindex]['#text']
                                    if self.isEnglish(comp) == 1:
                                        comp = comp.lower()
                                        ori = ori.lower()
                                    if comp in ori :
                                        tempAnd.remove(tempAnd[idx])

                                idx = self.isEnglish(titles[tindex]['#text'])
                                rawData[titleArr[idx]] =  titles[tindex]['#text']


                            # print('abs check')
                            for abs in absGroup :
                                for nkey in notKeywords :
                                    if nkey in abs['#text'] :
                                        raise Exception('not checked', abs['#text'][0:20])
                                for idx in range(len(tempAnd)-1,-1,-1) :
                                    comp = tempAnd[idx]
                                    ori = abs['#text']
                                    if self.isEnglish(comp) == 1:
                                        comp = comp.lower()
                                        ori = ori.lower()
                                    if comp in ori :
                                        tempAnd.remove(tempAnd[idx])
                                rawData[absDict[abs['@lang']]] = abs['#text']

                            if len(tempAnd) != 0 :
                                raise Exception('and checked')

                            # if 'abstract' not in rawData :
                            #     rawData[''] = ''
                            # print('JOURNAL check')
                            for jfield in fieldsJournal :
                                rawData[jfield] = journalGroup[fieldsJournal[jfield]]

                            # print('AFIED check')
                            for afield in fieldsArt :
                                rawData[afield] = articleInfo[fieldsArt[afield]]

                            names = ''
                            insts = ''
                            temp = ''

                            # print('AUTHOR check')
                            if isinstance(authorGroup, str) :
                                authorGroup = [authorGroup]
                            for author in authorGroup :
                                # getNameInst
                                if author[len(author) -1] != ')' :
                                    temp = author +' '
                                    continue
                                if len(temp) > 0 :
                                    author = temp + author
                                    temp = ''
                                idx =  author.find('(')

                                names += author[0:idx]+";"
                                insts += author[idx+1:len(author)-1]+';'

                            rawData['author']      = names[0:len(names)-1]
                            rawData['author_inst'] = insts[0:len(insts)-1]
                            tPro = now / total
                            if tPro >= 1.0 :
                                tPro = 0.99
                            rawData['progress'] = tPro

                            # rawData['progress']    = now / total

                            # pp(rawData)
                            """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                            if len(tempRaw) != 0 :
                                # print('send')
                                self.producer.send(self.site, value=tempRaw)
                                # produce.send(tempRaw)

                            tempRaw = rawData.copy()

                            for tl in titleArr :
                                rawData[tl] = ''
                            for abs in absDict :
                                rawData[absDict[abs]] = ''
                        except Exception as e:
                            # print(e)
                            #print('parsing Error')
                            numPerror += 1

                    print(p," end ",  (numPage-p) , ' remained' , ' Parsing Error : ', numPerror)
                    sleep(randint(1,3))



                    """ #5-5. kafka msg send => NTIS외에는 temp를 send (첫 결과 집합과 검색 도중 검색 결과 집합 개수가 다름)  """
                if len(tempRaw) != 0 :

                    if isLast :
                        tempRaw['progress'] = 1
                        tempRaw['isPartial'] = isPartial
                    print('send tempraw')
                    self.producer.send(self.site, value=tempRaw)
                    # produce.send(rawData)
                else :
                    if isLast :
                        self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'isPartial' : False})

                # return (now, total)
        except Exception as e:
            """ #5-6. 검색 실패시 """
            print(e)
            if isLast :
                self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'isPartial' : False})
                    #send error
            print(keyword, " error ")
        finally :
            print(isPartial, now, isLast)
            if isLast and now == 0 and isPartial:
                self.producer.send(self.site, value={'progress' : 1, 'keyId' : self.keyId, 'fail' : 1})
                print("crawl failed")
            self.flush()
            return (now, total)
    '''
    @ Method Name     	: noSite
    @ Method explain  	: 유효하지 않은 SITE 요청 인 경우
    '''
    def noSite(self):
        print('No site')

    myDict = {'KCI' : KCI, 'NTIS' : NTIS, 'DBPIA' : DBPIA, 'SCIENCEON' : NDSL , 'SCOPUS' : SCOPUS, 'WOS' : WOS}

__main__()
