import re, math, time, logging, datetime, sys, io
#from gensim.corpora import Dictionary
from sklearn.pipeline import Pipeline
#from gensim.models import TfidfModel
#from bson.objectid import ObjectId
from multiprocessing import Pool
#from gensim import similarities
from analyzer.analysis import Analysis
#from analyzer import Analysis
from random import randint
import scipy.sparse as sp



class analyzerProject(Analysis):
    def __init__(self, _keyId, _site, _query, _start, _end, _total):
        super().__init__(_keyId, _site, _query, _start, _end, _total)

    """
    @ Method Name     : 품질 / quality
    @ Method explain  : analyzerProject, analyzerPaper 에서 다시 정의 / Redefined in analyzer Project, analyzer Paper
    @ totalFunds      : 프로젝트 연구비 / project research funds
    """
    def quality(self, totalFunds):
        return totalFunds

    """
    @ Method Name     : cont
    @ Method explain  : 기여도 계산 함수 / Contribution calculation function
    @ _contBackdata   : getRawBackdata 함수에서 mngIds, A_ID 값을 가지고 있는 변수
    """
    def cont(self, _contBackdata):
        mngIds = _contBackdata['mngIds']
        A_ID   = _contBackdata['A_ID']
        point  = []
        for i in range(len(mngIds)):
            pt = 0
            temp = 0
            for j in range(len(mngIds[i])):
                if mngIds[i][j] != None:
                    if A_ID[i] == mngIds[i][j] :
                        pt += 10
                    else:
                        temp += 1
            if pt > 0 : 
                pt += temp
            point.append(pt)
        return point

    """
    @ Method Name     : getRawBackdata
    @ Method explain  : 전문가 지수를 계산하기 위해 필요로 하는 Backdata를 계산하는 함수 / A function that calculates the backdata needed to calculate the expert index.
    @ papers          : 논문(프로젝트) 수 / Number of papers (projects)
    @ A_ID            : 논문(프로젝트) 저자 고유 ID / Article (project) author unique ID
    """
    def getRawBackdata(self, papers, A_ID):
        pYears = []
        keywords = []
        totalFunds = []
        mngIds = []
        qry = []

        if (len(papers) != 0):
            for i in range(len(papers)-1, -1, -1):
                _pYear = []
                _keywords = []
                fund_list = []
                _mngIds = []
                __keyword = []
                for doc in self.Rawdata.find({"keyId": self.keyId, "_id": {"$in" : papers[i]}}):
                    fund_list.append(math.log(int(doc['totalFund'])+1))
                    _mngIds.append(doc['mngId'])

                    if doc['prdEnd'] != 'null':
                        _pYear.append(int(doc['prdEnd'][0:4]))
                    elif (doc['prdEnd'] == 'null') and (doc['prdStart'] != 'null'):
                        _pYear.append(int(doc['prdStart'][0:4]))
                    else:
                        _pYear.append(int(2000))
                    __keyword.append(doc['koTitle'])
                    __keyword.append(doc['enTitle'])
                    __keyword.append(doc['koKeyword'])
                    __keyword.append(doc['enKeyword'])
                if len(__keyword) != 0 :
                    _keywords.insert(0, __keyword)
                    totalFunds.insert(0, sum(fund_list))
                    mngIds.insert(0, _mngIds)
                    keywords.insert(0, _keywords)
                    pYears.insert(0, _pYear)
                else :
                    del A_ID[i]
                    del papers[i]
        return pYears, keywords, totalFunds, {'mngIds' : mngIds, 'A_ID' : A_ID}, None
