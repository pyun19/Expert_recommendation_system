from selenium.webdriver.common.alert import Alert
import requests, xmltodict, datetime, openpyxl, asyncio, functools
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
import datetime
from selenium.common.exceptions import ElementNotVisibleException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from collections import OrderedDict
from kafka import KafkaProducer
from langdetect import detect
from random import randint
from json import dumps
from time import sleep
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
     keyword = sys.argv[1:len(sys.argv)-2]   #// keyword = --k ~~~~ // year = --y 등 인자값 여려개일때 묶어야하나?
     keyId = int(sys.argv[len(sys.argv)-2])
     year = sys.argv[len(sys.argv)-1]
    #  site = sys.argv[len(sys.argv)-2]
    #  korea_option = int(sys.argv[len(sys.argv)-1])
     

     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year) # site, korea_option 삭제
     Producer(keyword, keyId, year).crawl()# site, korea_option 삭제

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
    @ site      		: 검색 사이트 // 삭제
    @ korea_option      : 한국 결과 / 종합 결과 flag (WOS / SCOPUS 만 사용) // 삭제
    '''

    def __init__(self, keyword, keyId, year, 
    ): # site, korea_option 삭제
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.site = "KCI"
        # self.korea_option = korea_option
        self.producer = KafkaProducer(bootstrap_servers= "203.255.92.141" + ':9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

       # print("korea", self.korea_option) #korea_option 삭제

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
        url                     = "https://open.kci.go.kr/po/openapi/openApiSearch.kci?"
        queryParams             = {'apiCode' : 'articleSearch', 'key' : "54974000", 'displayCount' : '100'}

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

    myDict = {'KCI' : KCI}

__main__()

