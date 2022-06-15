from selenium.webdriver.common.alert import Alert
import requests, xmltodict, datetime, openpyxl, asyncio, functools
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
import datetime
from SCOPUS import SCOPUS_crawler
from WOS import WOS_crawler
from ParsingHelper_SCOPUS import Parser
from ParsingHelper_SCOPUS import FileUpdateHandler
from ParsingHelper_SCOPUS import FileObserver
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
     keyword = sys.argv[1:len(sys.argv)-3]   #// keyword = --k ~~~~ // year = --y 등 인자값 여려개일때 묶어야하나?
     keyId = int(sys.argv[len(sys.argv)-3])
     year = sys.argv[len(sys.argv)-2]
     korea_option = int(sys.argv[len(sys.argv)-1])

     with open('/home/search/apps/product/etc/config.json') as f:
         config = json.load(f)

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year)
     Producer(keyword, keyId, year, korea_option, config).SCOPUS()

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

    def __init__(self, keyword, keyId, year, korea_option, config):
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.korea_option = korea_option
        self.config = config
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
#################################################################################################################SCOPUS
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
        fileObserver = FileObserver(path, crawl_end, parse_end, parse_data, num_data, total_data, self.keyId, isPartial, len(list), is_not_active, chrome_failed, self.config["IP"])


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

__main__ ()
