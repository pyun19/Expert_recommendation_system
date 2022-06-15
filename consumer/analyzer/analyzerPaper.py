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
from pymongo import MongoClient


class analyzerPaper(Analysis):
#    global AuthorRelation, QueryKeyword, AuthorPapers, ExpertFactor, Author, Rawdata, db2, public_QueryKeyword, KCI, SCI, ExpertFactorTable
    def __init__(self, _keyId, _site, _query, _start, _end, _total):
        self.oemList = ["Hyundai", "Kia","Toyota","Honda","Nissan","General Motors", "Chevrolet","Ford motor", "Volkswagen", "Audi", "BMW", "Bayerische Motoren Werke", "Mercedes-Benz", "daimler", "Volvo", "Renault", "Jaguar", "Acura", "Mazda", "Subaru", "Suzuki", "Isuzu","Daihatsu","Peugeot","Mclaren", "Bugatti", "Rolls Royce", "Bentley", "Aston Martin", "Land Rover", "Lotus","Lexus",   "Infiniti", "Datson", "Mitsubishi", "Mitsuoka","Great Wall","Cadillac", "Tesla", "Jeep", "Dodge", "Chrysler","Porsche", "Opel", "Borgward", "Gumfut", "FIAT", "Ferrari", "Lamborghini", "Maserati","Peugeot"]
        super().__init__(_keyId, _site, _query, _start, _end, _total)


    """
    @ Method Name     : coop
    @ Method explain  : 협업도 계산 함수 / Collaboration calculation function
    @ _contBackdata   : getRawBackdata 함수에서 mngIds, A_ID 값을 가지고 있는 변수 / Variables with mngIds and A_ID values in the getRawBackdata function
    """
    def coop(self, _coopBackdata):
        score = []
        for i in range(len(_coopBackdata)):
            point = 0
            for insts in _coopBackdata[i]:
                if insts != None :
                    for oem in self.oemList :
                        if oem in insts:
                            point = point + 1
                            break
            score.append(point)
        return score

    """
    @ Method Name     : quality
    @ Method explain  : analyzerPaper, analyzerProject에서 다시 정의 / Redefined in analyzer Project, analyzer Paper
    @ _contBackdata   : getRawBackdata 함수에서 issueInsts, issueLangs, citation 값을 가지고 있는 변수
    """
    def quality(self, _qtyBackdata):
        issueInsts = _qtyBackdata['issueInsts']
        issueLangs = _qtyBackdata['issueLangs']
        citation   = _qtyBackdata['citation']

        if self.site == 'WOS':
            global socialCitations
            socialCitations = _qtyBackdata['socialCitations']

        IF = []
        for i in range(len(issueInsts)):
            tempIF = 0
            for j in range(len(issueInsts[i])):
                temp = None
                tempIFIF = 0
                n = 1
                if issueLangs[i][j] == 'kor':
                    if isinstance(issueInsts[i][j], str) :
                        tempIFIF = self.kDic.get(issueInsts[i][j],0)
                else:
                    if isinstance(issueInsts[i][j], str) :
                        tempIFIF = self.sDic.get(issueInsts[i][j],0)
                    n = 3

                if self.site !='WOS':
                    tempIF += math.log(((citation[i][j]*n)+1) * (tempIFIF+1.1))
                else:
                    if socialCitations[i][j] == 0:
                        socialCitations[i][j] = 1
                    tempIF += math.log(((citation[i][j]*n)+1) * (tempIFIF+1.1)) + math.log(socialCitations[i][j])
            IF.append(tempIF)
        return IF
    
    """
    @ Method Name     : cont
    @ Method explain  : 기여도 계산 함수
    @ _contBackdata   : getRawBackdata 함수에서 mngIds, A_ID 값을 가지고 있는 변수
    """
    def cont(self, _contBackdata):
        authors = _contBackdata['authors']
        A_ID = _contBackdata['A_ID']
        aidToDict = {i : 0 for i in A_ID}

        for i in range(len(authors)):
            for j in  range(len(authors[i])) :
                x = authors[i][j].split(';')
                for author in enumerate(x):
                    if author[1] in aidToDict and author[1] == A_ID[i]:
                        if author[0] == 0:
                            aidToDict[author[1]] += 1.0
                        elif author[0] == len(x)-1:
                            aidToDict[author[1]] += 3.0
                        else :
                            aidToDict[author[1]] += ((author[0]+1)/len(x))
        return list(aidToDict.values())

    """
    @ Method Name     : getRawBackdata
    @ Method explain  : 전문가 지수를 계산하기 위해 필요로 하는 Backdata를 계산하는 함수
    @ papers          : 논문(프로젝트) 수
    @ A_ID            : 논문(프로젝트) 저자 고유 ID
    """
    def getRawBackdata(self, papers, A_ID):
#        global AuthorRelation, QueryKeyword, AuthorPapers, ExpertFactor, Author, Rawdata, db2, public_QueryKeyword, KCI, SCI, ExpertFactorTable
        pYears = []
        keywords = []
        authorInsts = []
        authors = []
        issueInsts = []
        issueLangs = []
        citation = []
        socialCitations = []

        for i in range(len(papers)-1, -1, -1):
            _pYear = []
            _keywords = []
            _keyword =   []
            _authorInsts =[]
            _authors =    []
            _issueInsts = []
            _issueLangs = []
            _citation = []
            _socialCitations = []
            for doc in self.Rawdata.find({"keyId": self.keyId, "_id": {"$in" : papers[i]}}):
                _keyword.append(doc['title'])
                _keyword.append(doc['english_title'])
                _keyword.append(doc['paper_keyword'])
                _keyword.append(doc['abstract'])
                _keyword.append(doc['english_abstract'])

                if self.site == 'WOS':
                    _socialCitations.append(doc['Usage Count'])

                if self.site == 'WOS':
                    if doc['issue_year'] == '':
                        doc['issue_year'] = 2000
                    _pYear.append(doc['issue_year'])
                else:
                    _pYear.append(int(doc['issue_year'][0:4]))

                _authorInsts.append(doc['author_inst'])
                _authors.append(doc['author_id'])
                _issueInsts.append(doc['issue_inst'])
                _issueLangs.append(doc['issue_lang'])

                if self.site == 'WOS' :
                    if doc['citation'] == '':
                        doc['citation'] = 0
                    _citation.append(doc['citation'])
                else:
                    _citation.append(int(doc['citation']))

            if len(_keyword) != 0 :
                authorInsts.insert(0,_authorInsts)
                authors.insert(0, _authors)
                issueInsts.insert(0, _issueInsts)
                _keywords.insert(0,_keyword)
                pYears.insert(0,_pYear)
                issueLangs.insert(0,_issueLangs)
                keywords.insert(0,_keywords)
                citation.insert(0,_citation)

                if self.site == 'WOS':
                    socialCitations.insert(0, _socialCitations)
            else :
                del A_ID[i]
                del papers[i]

        if self.site != 'WOS':
            return pYears, keywords, {'issueInsts' : issueInsts, 'issueLangs' : issueLangs, 'citation' : citation}, {'authors' : authors, 'A_ID' : A_ID  }, authorInsts #for coop
        else:
            return pYears, keywords, {'issueInsts' : issueInsts, 'issueLangs' : issueLangs, 'citation' : citation, 'socialCitations':socialCitations}, {'authors' : authors, 'A_ID' : A_ID  }, authorInsts #for coop
