
from multiprocessing import Process, Queue
import multiprocessing
import os
from time import sleep
from bson.objectid import ObjectId
from pymongo import MongoClient
from new_analyzer import factor_integration
import sys
# def __main__():
#     f_id = 0 #input
#     keyid = 674
#     analyzer = run_factor_integration(keyid, f_id)
    
#     analyzer.run()
#    # analyzer.factor_norm()

class run_factor_integration:
    def __init__(self, keyid, fid):
        self.cores = multiprocessing.cpu_count()
        if self.cores > 3 :
            self.cores -= 1
        self.client =  MongoClient('203.255.92.141:27017', connect=False)
        self.PUBLIC = self.client['PUBLIC']
        self.new_max_factor = self.PUBLIC['new_factor'] 
        self.ID = self.client['ID']
        self.Domestic = self.ID['Domestic']
        self.keyid = keyid
        self.fid = fid

    def run(self):
        authorSize = self.ID['Domestic'].find({"keyId":self.keyid, "fid":self.fid}).count()
        processList = []
        if None == self.new_max_factor.find_one({'keyId': self.keyid,"fid":self.fid}):
            self.new_max_factor.insert({'keyId': self.keyid,"fid":self.fid},{'keyId': self.keyid,"fid":self.fid, 'Quality' : -1, 'accuracy' : -1, 'recentness' : -1, 'coop': -1 })
        
        th = 100 # each core handle 100 or more data
        sizeDict = {}
        perData = int(authorSize / self.cores)
        if perData > th :
            last = 0
            for i in range(self.cores-1) :
                sizeDict[last] = last+perData
                last += perData
            sizeDict[last] = authorSize
        else :
            sizeDict[0] = authorSize
        # 이 코드는 코어수를 기반으로 총 data를 나눠준다. 하지만 총 데이터가 100개 이하이면 나누지 않는다.
      #  print(sizeDict)
        
        processList = []
        for key in sizeDict :
            acl = None
            acl = factor_integration(key, sizeDict[key], self.fid ,self.keyid)


            p = Process(target= acl.run)
            processList.append(p)
            p.start()
        for p in processList :
            p.join()

        self.factor_norm()

    def factor_norm(self):
        max_factor = self.new_max_factor.find_one({'keyId':self.keyid})
        
        max_qual = max_factor['Quality']
        real_qual =1/ max_qual
        
        max_acc = max_factor['accuracy']
        max_recentness = max_factor['recentness']
        max_coop = max_factor['coop']
        real_recentness = 1/ max_recentness
        if max_coop == 0: real_coop = max_coop
        elif max_coop != 0: real_coop = 1/ max_coop
        self.ID['test'].update_many({"keyId":self.keyid, "fid":self.fid}, [{"$set" :
         {"factor.coop":{"$multiply": ["$factor.coop", real_coop]},
          "factor.qunt":{"$sum":[ {"$multiply": [real_recentness, "$factor.qunt", 0.5]}, "$factor.lct"]},
          "factor.qual":{"$multiply": ["$factor.qual", real_qual]},
        "score" : {'$sum':[{"$multiply": ["$factor.qual", real_qual, 25]},{"$multiply": ["$factor.acc", 25]},{"$multiply": ["$factor.coop", real_coop, 25]},
        {"$multiply": [{"$sum":[ {"$multiply": [real_recentness, "$factor.qunt", 0.5]}, "$factor.lct"]}, 25]}]}}}])

        print("종료")


# #[{"$multiply": ["$factor.qual", real_qual, 25]}]
#  db.test.updateMany({}, [

# {"$set" : 
# {"score_t" : 
# {'$sum':[{"$multiply": ["$factor.qual", 100, 25]}, {"$multiply": ["$factor.acc", 25]},{"$multiply": ["$factor.coop", 25]},{"$multiply": ["$factor.qunt", 25]}]

# }}}])