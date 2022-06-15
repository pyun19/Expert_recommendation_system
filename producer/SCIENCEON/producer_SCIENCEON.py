m selenium.webdriver.common.alert import Alert
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
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Value, Array, Lock

logging.basicConfig()
logger = logging.getLogger('retry_count')
logger.setLevel(logging.INFO)

def __main__ ():     
     """ #1. 파라미터 파싱 """
     keyword = sys.argv[1:len(sys.argv)-2]   
     keyId = int(sys.argv[len(sys.argv)-2])
     year = sys.argv[len(sys.argv)-1]

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year)
     Producer(keyword, keyId, year).crawl()

""" #2. Producer 객체 생성 (crawler)"""
class Producer:
    # Kafka 서버 IP => Porting 시 수정
    producer = KafkaProducer(bootstrap_servers='203.255.92.141:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    '''
	@ Method Name		: __init__
	@ Method explain	: 클래스 생성자
	@ keyword     	  	: 입력 키워드 
	@ keyId     	  	: 검색 ID
	@ year				: 검색 년도		
    '''

    def __init__(self, keyword, keyId, year):
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.site = "NDSL"


        """ Note #1 : 10만건 이상 검색 시 stop """
        self.stopCount = 100000
        self.timeout = 60
        self.temp = {}

        """ #3. API에 사용될 입력 키워드 파싱 """
        self.keywords = {'and' : [], 'or' : [], 'not' : []}
        for k in self.keyword :
            type = 'and'
            if ('|' in k)   == True: # or 연산               
                type = 'or'                 
            elif ('!' in k) == True: # not 연산                 
                type = 'not'            

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



            """ Note #3. NDSL Helper """
    '''
    @ Method Name       : dividePage
    @ Method explain    : NDSL 논문 page 값 분할
    @ s                 : page string 값 
    '''
    
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
    @ Method Name       : NDSL
    @ Method explain    : NDSL Crawling 실행  
    '''
    def NDSL(self):
        dt = datetime.datetime.now()

        # 발행년도 Filter code #
        # API Key 변경시 수정
        url = "http://openapi.ndsl.kr/itemsearch.do?keyValue=02570602&target=NART&searchField=&displayCount=100"

        for y in range(int(self.year), int(dt.year)+1) :
            semi_url = url + "&sortby=pubyear&returnType=json&responseGroup=simple&query=(BI:" + self.apiKeyword +") and (PY:"+ str(y) + ")&callback=callback"
            
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

                                    
                                    tPro = a/b + (cnt / total) / b
                                    

                                    if tPro >= 1.0 :
                                        tPro = 0.99
                                    rawData['progress'] = tPro
                                    
                                    self.producer.send(self.site, rawData)


                                    if temp_rawData is not None:
                                        self.producer.send(self.site, temp_rawData)


                                    temp_rawData = rawData.copy()
                                except Exception as e :
                                   
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


    '''
    @ Method Name       : noSite
    @ Method explain    : 유효하지 않은 SITE 요청 인 경우  
    '''
    def noSite(self):
        print('No site')

    myDict = {'SCIENCEON' : NDSL}

__main__()

