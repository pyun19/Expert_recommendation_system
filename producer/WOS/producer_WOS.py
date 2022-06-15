#-*- coding:utf-8 -*-
# my_pc : z40kj740zp2w34tuql0a
# server_pc : hwbpt09z26145s5ca2cz
# pip install requests
# pip install bs4
# pip install selenium
import shutil
import os.path
import sys, os, re, math, logging, time, json, xlrd, retry, pprint

#cd /home/search/apps/topher/
#python3 producer_WOS.py korea 10007 2010 1
from json import dumps
from time import sleep
from WOS import WOS_crawler
from threading import Thread
from selenium import webdriver
from kafka import KafkaProducer
from ParsingHelper_WOS import Parser
from ParsingHelper_WOS import FileObserver
from multiprocessing import Process, Value
from ParsingHelper_WOS import FileUpdateHandler


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

     with open('../etc/config.json') as f:
         config = json.load(f)

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year)
     Producer(keyword, keyId, year, korea_option, config).WOS()

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


    """ #4. crawling 시작 """
    '''
    @ Method Name       : crawl
    @ Method explain    : 각 사이트 별 수집기 실행
    '''

    '''
    @ Method Name       : WOS_crawler_start
    @ Method explain    : WOS 크롤링 동작 함수
    @ crawl_end         : crawling 종료(True: End, False: not yet)
    @ parse_end         : parsing 종료(True: End, Flase: not yet)
    @ parse_data        : parsing된 파일 수
    @ num_data          : download file 수
    @ total_data        : 첫 검색 결과 논문 수
    @ path              : download 파일 저장 위치
    @ isPartial         : 부분 crawling 여부 저장
    @ is_not_active     : HTTP code확인(403 error) (1 = True / 0 = False) <-- 한번에 많은 데이터를 다운로드 받거나 기계적 접근이 감지 되면 차단당함
    @ chrome_failed     : chome ERROR 체크
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

        fileObserver = FileObserver(path, crawl_end, parse_end, parse_data, num_data, total_data, self.keyId, isPartial, numProcess, is_not_active, chrome_failed, self.config["IP"])
        p = Process(target=self.WOS_crawler_start, args=(crawl_end, parse_end, parse_data, num_data, total_data, path, isPartial, is_not_active, chrome_failed))
        p.start()

        p2 = Process(target=fileObserver.run)
        p2.start()

        p.join()
        p2.join()



__main__()
