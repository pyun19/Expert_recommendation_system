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
     with open('../etc/config.json') as f:
         config = json.load(f)

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year)
     Producer(keyword, keyId, year, config).crawl()

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

    def __init__(self, keyword, keyId, year, config):
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.site = "DBPIA"
        self.config = config
        self.producer = KafkaProducer(bootstrap_servers= self.config["IP"] + ':9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))


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
    def noSite(self):
        print('No site')

    myDict = {'DBPIA' : DBPIA}

__main__()
