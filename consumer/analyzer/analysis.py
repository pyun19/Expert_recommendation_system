import re, math, time, threading, logging, datetime, sys, io, queue
from sklearn.feature_extraction.text import HashingVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfVectorizer
#from gensim.corpora import Dictionary
from sklearn.pipeline import Pipeline
#from gensim.models import TfidfModel
from bson.objectid import ObjectId
from multiprocessing import Pool
from pymongo import MongoClient
#from gensim import similarities
from numpy.linalg import norm
from threading import Thread
from random import randint
import scipy.sparse as sp
from time import sleep
from numpy import dot
import pandas as pd
import numpy as np




""" #1. 분석기 생성 """
class Analysis(threading.Thread):
    
    client =  MongoClient('localhost:27017', connect=False)
    db = None
    dt = datetime.datetime.now()
    AuthorRelation = None
    QueryKeyword = None
    AuthorPapers  = None
    ExpertFactor  = None
    Author = None
    Rawdata = None
    db2  = None
    public_QueryKeyword  = None
    KCI  = None
    SCI  = None
    ExpertFactorTable  = None
    """
    @ Method Name     : __init__
    @ Method explain  : Analysis 생성자를 만들어주는 함수
    @ _keyId          : 해당 쿼리 keyId
    @ _site           : 사이트명 
    @ _query          : 쿼리키워드(Ex. fuel cell)
    @ _start          : key (컨슈머 runAnalazer 함수 )
    @ _end            : sizeDict[key]
    @ _total          : 해당 KeyId의 authorPapers 전체 수
    """
    def __init__(self, _keyId, _site, _query, _start, _end, _total):
        threading.Thread.__init__(self)
        self.keyId        = _keyId
        self.site         = _site
        self.defaultScore = 0.02
        self.start        = _start
        self.end          = _end
        self.total        = _total
        self.query        = _query

    def initDBs(self) :
#        global AuthorRelation, QueryKeyword, AuthorPapers, ExpertFactor, Author, Rawdata, db2, public_QueryKeyword, KCI, SCI, ExpertFactorTable
#        db                  = self.client[self.site]
#        db2                 = self.client.PUBLIC
#        public_QueryKeyword = db2.QueryKeyword
#        AuthorRelation      = db.AuthorRelation
#        QueryKeyword        = db.QueryKeyword
#        AuthorPapers        = db.AuthorPapers
#        ExpertFactor        = db.ExpertFactor
#        Author              = db.Author
#        Rawdata             = db.Rawdata
#        KCI                 = db2.KCI
#        SCI                 = db2.SCI
#        ExpertFactorTable   = db.ExpertFactorTable
#
#        self.kDic = {}
#        self.sDic = {}
#        for doc in KCI.find({}) :
#            self.kDic[doc['name']] = doc['IF']
#        for doc in SCI.find({}) :
#            self.sDic[doc['name']] = doc['IF']
        db                  = self.client[self.site]
        db2                 = self.client.PUBLIC
        self.public_QueryKeyword = db2.QueryKeyword
        self.AuthorRelation      = db.AuthorRelation
        self.QueryKeyword        = db.QueryKeyword
        self.AuthorPapers        = db.AuthorPapers
        self.ExpertFactor        = db.ExpertFactor
        self.Author              = db.Author
        self.Rawdata             = db.Rawdata
        self.KCI                 = db2.KCI
        self.SCI                 = db2.SCI
        self.ExpertFactorTable   = db.ExpertFactorTable

        self.kDic = {}
        self.sDic = {}
        for doc in self.KCI.find({}) :
            self.kDic[doc['name']] = doc['IF']
        for doc in self.SCI.find({}) :
            self.sDic[doc['name']] = doc['IF']
    """
    @ Method Name     : recentness
    @ Method explain  : 최신성 지수 계산 함수 
    @ pYears          : (1) NTIS : 프로젝트 시작년도 (2) 나머지 : 논문 발행년도
    """
    # 최신성 계산
    def recentness(self, pYears):
        rct_list = []
        for i in range(len(pYears)):
            rct = 0
            for j in range(len(pYears[i])):
                if pYears[i][j] >= int(self.dt.year)-2: # 최신년도 기준으로 과거 2년까지 +1점
                    rct += 1
                elif int(self.dt.year)-15 < pYears[i][j] <= int(self.dt.year)-3: # 최신년도 기준 과거 15년 ~ 과거 2년까지 
                    rct += max(round((1-(int(self.dt.year)-3-pYears[i][j])*0.1),2), 0)
                else:
                    rct += 0
            rct_list.append(rct / len(pYears[i]))
        return rct_list

    """
    @ Method Name     : career
    @ Method explain  : 경력을 계산해 주는 함수
    @ pYears          : (1) NTIS : 프로젝트 시작년도 (2) 나머지 : 논문 발행년도
    """
    def career(self, pYears):
        crr_list = []
        for i in range(len(pYears)):
            _max = max(pYears[i])
            _min = min(pYears[i])
            crr = _max-_min+1
            crr_list.append(crr)
        return crr_list
        
    """
    @ Method Name     : durability
    @ Method explain  : 연구지속성 계산해 주는 함수
    @ pYears          : (1) NTIS : 프로젝트 시작년도 (2) 나머지 : 논문 발행년도
    """
    def durability(self, pYears):
        maxLen = []
        for i in range(len(pYears)):
            pYears[i].sort(reverse=True)
            packet = []
            tmp = []
            v = pYears[i].pop()
            tmp.append(v)
            while(len(pYears[i])>0):
                vv = pYears[i].pop()
                if v+1 == vv:
                    tmp.append(vv)
                    v = vv
                elif v == vv:
                    pass
                else:
                    packet.append(tmp)
                    tmp = []
                    tmp.append(vv)
                    v = vv
            packet.append(tmp)
            maxLen.append(packet)

        xx_list = []
        for i in range(len(maxLen)):
            x = []
            for j in range(len(maxLen[i])):
                x.append(len(maxLen[i][j]))
            xx_list.append(max(x))
        return xx_list


    """
    @ Method Name     : qty
    @ Method explain  : 생산성 계산 함수
    @ papers          : 논문(프로젝트) 수 
    """
    def qty(self, papers):
        qt = []
        for i in range(0,len(papers)):
            cnt = 0
            cnt = len(papers[i])
            qt.append(cnt)
        return qt


    """
    @ Method Name     : acc
    @ Method explain  : 정확도 계산 함수
    @ keywords        : 1) 논문 제목, 프로젝트 키워드 (in project) 
                        2) 논문 제목, 논문 키워드, abstract (in paper)
    @ contBit         : contrib 값에서 0을 제외한 값
    """
    def acc(self, keywords, contBit):
        rtv = contBit.copy()
        for i in range(len(keywords)):
            try :
                if rtv[i] != 0:
                    temp = self.calAcc(keywords[i])
                    if temp == 0.0 :
                        rtv[i] = self.defaultScore
                    else :
                        rtv[i] = temp
            except Exception as e :
                print(keywords[i])
                print(e)
        return rtv


    """
    @ Method Name     : calAcc
    @ Method explain  : 정확도 계산 함수(실제 acc 함수에서 각 논문/프로젝트 정확도 계산)
    @ keywords        : 1) 논문 제목, 프로젝트 키워드 (in project) 
                        2) 논문 제목, 논문 키워드, abstract (in paper)
    """
    def calAcc(self, keywords):
        flat_list = []
        for sublist in keywords :
            for item in sublist :
                if item is not None and item != 'None' and item != "" and isinstance(item, str) :
                    flat_list.append(item)
        if len(flat_list) == 0 :
            return 0 

        qs = self.query.split()
        qs = [_qs for _qs in qs if len(_qs) >= 2]
        tfidf_vectorizer = TfidfVectorizer (analyzer='word', ngram_range=(1, 1))
        tfidf_vectorizer.fit([self.query])

        arr = tfidf_vectorizer.transform(flat_list).toarray()
        qrytfidf = [1] *len(qs)
        if sum(arr[np.argmax(arr.sum(axis=1))]) != 0:
            return self.cos_sim(arr[np.argmax(arr.sum(axis=1))], qrytfidf)
        else :
            return 0


    """
    @ Method Name     : cos_sim
    @ Method explain  : 코사인 유사도 함수 
    @ A               : calAcc 함수에서 arr[np.argmax(arr.sum(axis=1))]
    @ B               : calAcc 함수에서 qrytfidf
    """
    def cos_sim(self, A, B):
        return dot(A, B)/(norm(A)*norm(B))


    """
    @ Method Name     : storeExpertFactors
    @ Method explain  : 정확도 계산 함수
    @ A_ID            : 논문(프로젝트) 저자 고유 ID
    @ rctt            : recentness 함수를 사용하여 나온 값
    @ crrt            : career 함수를 사용하여 나온 값
    @ durat           : durability 함수를 사용하여 나온 값
    @ contrib         : cont 함수를 사용하여 나온 값
    @ qual            : quality 함수를 사용하여 나온 값
    @ qt              : qty 함수를 사용하여 나온 값
    @ accuracy        : acc 함수를 사용하여 나온 값
    @ coop            : coop 함수를 사용하여 나온 값
    @ contBit         : contrib 값에서 0을 제외한 값
    @ papers          : 논문(프로젝트) 수    
    """
    def storeExpertFactors(self, A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy, coop, contBit, papers):
        expf = []
        for i in range(len(A_ID)):
            exp = {}
            exp['A_ID']         = A_ID[i]
            exp['keyId']        = self.keyId
            exp['Productivity'] = qt[i] * contBit[i]
            exp['Contrib']      = contrib[i]
            exp['Durability']   = (durat[i]/crrt[i]) * contBit[i]
            exp['Recentness']   = rctt[i] * contBit[i]
            if coop is None :
                exp['Coop']     = 0
            else :
                exp['Coop']     = coop[i] * contBit[i]
            exp['Quality']      = qual[i] * contBit[i]
            exp['Acc']          = accuracy[i]
            exp['Numpaper']     = len(papers[i])
            expf.append(exp)
        x = self.ExpertFactor.insert_many(expf)

    """
    @ Method Name     : getBackdata
    @ Method explain  : 크롤링 한 결과가 저장되 있는 DB에서 A_ID, papers 값 도출하는 함수
    @ i               : run 함수에서 실행되는 반복문의 i 값
    @ dataPerPage     : dataPerPage(100)
    """
    def getBackdata(self, i, dataPerPage):
        #global AuthorPapers
        A_ID    = []
        papers  = []
        sCount  = self.start + (i*dataPerPage)
        lCoount = min(dataPerPage, self.end - sCount )
        for doc in self.AuthorPapers.find({"keyId":self.keyId}).skip(sCount).limit(lCoount): #31323
            A_ID.append(doc['A_ID'])
            papers.append(doc['papers'])
        return  A_ID, papers


    """
    @ Method Name     : quality
    @ Method explain  : analyzerProject, analyzerPaper 에서 다시 정의
    """
    def quality(self, _qtyBackdata):
        pass

    """
    @ Method Name     : cont
    @ Method explain  : analyzerProject, analyzerPaper 에서 다시 정의
    """
    def cont(self, _contBackdata):
        pass

    """
    @ Method Name     : coop
    @ Method explain  : analyzerProject, analyzerPaper 에서 다시 정의
    """
    def coop(self, _coopBackdata):
        pass

    """
    @ Method Name     : getRawBackdata
    @ Method explain  : analyzerProject, analyzerPaper 에서 다시 정의
    """
    def getRawBackdata(self, papers, A_ID):
        pass


    """multi thread code"""
    # threadList = ["t1", "t2"]
    # threads = []
    # workQueue = queue.Queue()
    # queueLock.acquire()
    # for i in range(math.ceil(All_count/dataPerPage)) :
    # # for word in nameList:
    #    workQueue.put(i)
    # queueLock.release()
    # for tname in threadList:
    #     t = Thread(target=self.workingThreading, args=(workQueue, All_count, qry_result, factorVars))
    #     t.start()
    #     threads.append(t)
    #
    #
    #
    # # Wait for queue to empty
    # while not workQueue.empty():
    #     print(workQueue)
    #     sleep(1)
    #     pass
    #
    # # Notify threads it's time to exit
    # exitFlag = 1
    #
    # # Wait for all threads to complete
    # print("waiting join")
    # for t in threads:
    #    t.join()
    # print ("Exiting Main Thread")


    """ #2. 분석기 실행 """
    def run(self):

        """ #3. DB client 생성 """
        self.initDBs()
        All_count = self.end - self.start
        dataPerPage = 100

        self.dt = datetime.datetime.now()
        print('start', self.dt)
        tempQty  = -1
        tempCont = -1
        tempQual = -1
        tempCoop = -1
        maxFactors = {'Quality' : -1, 'Productivity' : -1, 'Contrib' : -1 }
        factorVars = {'Quality' : 'tempQual', 'Productivity' : 'tempQty', 'Contrib' : 'tempCont' }
        if self.site != 'NTIS' :
            maxFactors['Coop'] = -1
            factorVars['Coop'] = 'tempCoop'

        """ #4. 지수별 분석 실행 """
        allPage = math.ceil(All_count/dataPerPage)
        for i in range( allPage):
            A_ID, papers = self.getBackdata(i, dataPerPage)
            (pYears, keywords, _qtyBackdata, _contBackdata, _coopBackdata) = self.getRawBackdata(papers, A_ID)

            if len(pYears) > 0 :
                contrib  = self.cont(_contBackdata)
                contBit  = [1 if j > 0 else j for j in contrib]
                rctt     = self.recentness(pYears)
                crrt     = self.career(pYears)
                durat    = self.durability(pYears)
                qt       = self.qty(papers)
                coop     = self.coop(_coopBackdata)
                qual     = self.quality(_qtyBackdata)
                accuracy = self.acc(keywords, contBit)

                tempQty  = max(qt)
                tempCont = max(contrib)
                tempQual = max(qual)

                if self.site != 'NTIS' :
                    tempCoop = max(coop)

                for f in maxFactors :
                    if maxFactors[f] < eval(factorVars[f]) :
                        maxFactors[f] = eval(factorVars[f])

                exft = self.storeExpertFactors(A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy, coop, contBit, papers)
            progress = (len(A_ID)/self.total) * 100.0
            self.QueryKeyword.find_one_and_update({"_id":self.keyId},{'$inc':{"progress":progress}})

        """ #5. 분석기 종료 """
        self.dt = datetime.datetime.now()
        print('end',self.dt)

        """ #6. 지수 별 최대 값 update (for normalization) """
        if self.site !='NTIS' and maxFactors['Coop'] == 0:
            maxFactors['Coop'] = 1

        self.ExpertFactorTable.update({"_id" : self.keyId}, {"$max" : maxFactors})
        print(maxFactors)


        """ #7. 현재 process가 최종 분석을 수행한 놈인지 확인 """
        # aedData = ExpertFactor.count({"keyId":self.keyId})
        # print(aedData)
        # if aedData == self.total : #last processor
        #     finMaxEf = ExpertFactorTable.find_one({"_id" : self.keyId})
        #     print(finMaxEf)
        #     setOpts = []
        #
        #     """ #8-1. normalization """
        #     for k in maxFactors :
        #         tempStr = { "$set" : { k  : { "$multiply" : [1/finMaxEf[k], "$"+k] }}}
        #         setOpts.append(tempStr)
        #     print(setOpts)
        #     ExpertFactor.update_many({"keyId":self.keyId}, setOpts)
        #
        #
        #     print("normaliztioned")
        #     """ #9. 분석 완료 """
        #     QueryKeyword.update({"_id":self.keyId},{'$set':{"progress":100, "state":2, "experts" : self.total, "a_time" : self.dt.strftime("%Y-%m-%d %H:%M:%S")}})
        #     print('analyzer end')

        """ #8-2. 작업 완료 (start, end) => 할당 받은 시작 위치, 종료 위치  """
        print("s : " , self.start,  ", e : ", self.end,  " ExpertFactor 성공 ")
        return "anaylzer on"

###################################################################################################################################################################현재 미사용 함수

    """
    @ Method Name     : minmaxNorm
    @ Method explain  : 현재 미사용
    """
    def minmaxNorm(self, arr):
       rtv = []
       max_val = max(arr)
       min_val = min(arr)
       norm =  max_val-min_val
       for i in range(0, len(arr)):
           if arr[i] == min_val:
               rtv.append(self.defaultScore)
           else :
               rtv.append((arr[i]-min_val)/norm)
       return rtv


    """
    @ Method Name     : removeSkeywords
    @ Method explain  : 현재 미사용
    """
    def removeSkeywords(self, strArr, min_support):
        rtv = []
        for i in range(len(strArr)):
            temp = re.sub('[-=.#/?:$}()]','', str(strArr[i]))
            if temp != ('')  and temp != 'None' and len(temp) >= min_support:
                rtv.append(temp)
        return rtv


    """
    @ Method Name     : test_func
    @ Method explain  : 현재 미사용
    """
    def test_func(self, data) :
        tfidf_vectorizer = TfidfVectorizer(analyzer='word', ngram_range=(1, 1))
        flat_list = [item for sublist in data for item in sublist]
        tfidf_matrix = tfidf_vectorizer.transform(flat_list)
        return tfidf_matrix


    """ not used now  """
    def workingThreading(self, q, a, qry_result,factorVars ) :
        while not q.empty():
            print("workwell")
            try :
                queueLock.acquire()
                if not q.empty() :
                    print("workwell2")
                    i = q.get()
                    queueLock.release()
                    A_ID, papers = self.getBackdata(i, dataPerPage)
                    (pYears, keywords, _qtyBackdata, _contBackdata, _coopBackdata) = self.getRawBackdata(papers, A_ID)

                    if len(pYears) > 0 :
                        contrib  = self.cont(_contBackdata)
                        contBit  = [1 if i > 0 else i for i in contrib]
                        rctt     = self.recentness(pYears)
                        crrt     = self.career(pYears)
                        durat    = self.durability(pYears)
                        qt       = self.qty(papers)
                        coop     = self.coop(_coopBackdata)
                        qual     = self.quality(_qtyBackdata)
                        accuracy = self.acc(qry_result, keywords, A_ID, contBit)

                        tempQty  = max(qt)
                        tempCont = max(contrib)
                        tempQual = max(qual)

                        if self.site != 'NTIS' :
                            tempCoop = max(coop)

                        for f in maxFactors :
                            if maxFactors[f] < eval(factorVars[f]) :
                                maxFactors[f] = eval(factorVars[f])

                        exft = self.storeExpertFactors(A_ID, rctt, crrt, durat, contrib, qual, qt, accuracy, coop, contBit)
                    progress = i/math.ceil(a/dataPerPage+1) * 100.0
                    QueryKeyword.update({"_id":self.keyId},{'$set':{"progress":progress}})
                sleep(0.1)
            except Exception as e:
                print(e)
