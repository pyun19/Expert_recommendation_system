#-*- coding:utf-8 -*-
# my_pc : z40kj740zp2w34tuql0a
# server_pc : hwbpt09z26145s5ca2cz
# pip install requests
# pip install bs4
# pip install selenium
import requests, xmltodict, datetime, openpyxl, asyncio, functools
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
import datetime
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
     keyword = sys.argv[1:len(sys.argv)-2]   
     keyId = int(sys.argv[len(sys.argv)-2])
     year = sys.argv[len(sys.argv)-1]
     
     print ("Keyword :", keyword, "/ KeyID :", keyId, "/ Year : ", year)
     Producer(keyword, keyId, year).crawl()

class Producer:

    producer = KafkaProducer(bootstrap_servers='203.255.92.141:9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))

    def __init__(self, keyword, keyId, year):
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.site = "NTIS"

        self.stopCount = 100000
        self.timeout = 60
        self.temp = {}

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

    def flush(self):
        self.producer.flush()
	
    def crawl(self):
        print("crawl")
        name = self.site
        f = lambda self, x : self.myDict.get(x, lambda x : self.noSite())(self)
        f(self, name)

    def ntis_remove_html_tags(self, data):
        if data == 'None':
            pass            
        else:
            p = re.compile(r'<.*?>||-')
            return p.sub('', str(data))

    def NTIS(self):        
        self.dt = datetime.datetime.now()

        url = "https://www.ntis.go.kr/rndopen/openApi/pjtSearch/project"
        self.queryParams = '?' + 'apprvKey=' + 'hwbpt09z26145s5ca2cz' \
            + '&collection=' + 'project' + '&SRWR=' + self.apiKeyword \
            + '&searchRnkn=' + 'DATE/DESC' + '&displayCnt=' + '100' + '&addquery=PY=' + self.year + "/MORE"
        self.url = url + self.queryParams

        rescode = 0
        try :
            resq = requests.get(self.url, timeout=self.timeout)
            rescode = resq.status_code
        except Exception as e :
            print(e)
            rescode = 500
        
        if rescode == 200:
            conn = resq.content
            self.xmlDict = xmltodict.parse(conn)
            print(self.url)

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
                    pageNumb = int(math.floor((int(self.pageNum)/100)))
                    print('PAGE NUMBER >> ', (pageNumb))
                    rawData = {}

                    START =  datetime.datetime.now()
                    for i in range(0, pageNumb+1):
                        try :
                            if cnt > self.stopCount :
                                isPartial = True
                                break
								
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
                                # print(final_xmlDict['RESULT']['RESULTSET']['HIT'])
                                for HIT in final_xmlDict['RESULT']['RESULTSET']['HIT']:
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
                                    rawData['odAgency']  = HIT['OrderAgency']['Name']
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
                                    # print(rawData)                                    
                                    self.producer.send(self.site, rawData)
                        except Exception as e :
                            print(e)                            
                            numPerror += 1
                        print(i, " page Comp.", (pageNumb-i), ' remained', ' Num Parsing Error : ', numPerror )

                    END =  datetime.datetime.now()
                    print("Elapsed Time : ", (END-START), pageNumb)                    
                    if 'progress' in rawData and rawData['progress'] != 1:
                        self.producer.send(self.site, {"keyId" : self.keyId, 'progress' : 1, 'isPartial' : False})
                    self.flush()

        else:
            nothing['progress'] = 1.0
            nothing['keyId'] = self.keyId
            nothing['fail'] = 1
            self.producer.send(self.site, value=nothing)
            print("Error Code:" + rescode)

    def noSite(self):
        print('No site')

    myDict = {'NTIS' : NTIS}

__main__()
