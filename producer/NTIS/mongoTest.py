from pymongo import MongoClient

client       =  MongoClient('203.255.92.141:27017', connect=False)
QueryKeyword = "QueryKeyword"
NTIS         = client['NTIS']
expertFactor = NTIS['ExpertFactor']
author       = NTIS['Author']
rawData      = NTIS['Rawdata']
isCollect    = False

def __main__():
    num_experts = 0 # 저자 정보 수집 대상
    com_key     = get_common_keyid(domestic_option = 1)
    for key in com_key:
        # print(NTIS[QueryKeyword].find_one({"_id":key}))
        if 'isCollect' not in NTIS[QueryKeyword].find_one({"_id":key}):
            x = NTIS[QueryKeyword].find_one({"_id":key}, {"experts":1})
            num_experts += x['experts']

    for key in com_key:
        print("key : ", key)
        ls = list(expertFactor.find(
            {
                'keyId':key,
                "Productivity":{"$gt":0}, 
            },
            {
                "_id" : 0, 
                "keyId" : 1, 
                "A_ID" : 1, 
                "Productivity": 1, 
                "contirb" : 1, 
                "Durability" : 1, 
                "Recentness" : 1, 
                "Quality" : 1, 
                "Acc" : 1 
            }))

        for _id in ls:
            __id = author.find_one({'_id':_id['A_ID']})
            if __id is not None and 'isCollect' not in __id:
                result = rawData.find_one({'keyId':key,'mngId':_id['A_ID']},{'keyId':1,'mng':1,'ldAgency':1,'koTitle':1,'enTitle':1, '_id':0})
                # 크롤러 수집기 붙이는 곳

def get_common_keyid(domestic_option = 1):
    """mongoDB에서 공통keyid를 추출 하는 함수
    
    :para domestic_option = 1 국내
    :para domestic_option = 2 국외
    :para domestic_option = 3 ALL
    """
    domestics = ['NTIS', 'SCIENCEON', 'KCI', 'DBPIA']
    overseas = ['SCOPUS', 'WOS']
    ALL = domestics + overseas
    siteDict = {i : []  for i in ALL} 
    if domestic_option == 1:
        for site in domestics:
            db = client[site] #print(client[site])
            for _ in list(db[QueryKeyword].find({},{'_id':1})):
                siteDict[site].append(_['_id'])
    
        vs1 = set(siteDict['NTIS']) & set(siteDict['SCIENCEON'])
        vs2 = set(siteDict['DBPIA']) & set(siteDict['KCI'])
        vsResult = list(vs1 & vs2)
        vsResult.sort(reverse=True)

        return vsResult

    elif domestic_option == 2:
        for site in overseas:
            db = client[site] #print(client[site])
            for _ in list(db[QueryKeyword].find({},{'_id':1})):
                siteDict[site].append(_['_id'])

        vs = set(siteDict['SCOPUS']) & set(siteDict['WOS'])
        vsResult = list(vs)
        vsResult.sort(reverse=True)

        return vsResult

    else:
        for site in ALL:
            db = client[site] #print(client[site])
            for _ in list(db[QueryKeyword].find({},{'_id':1})):
                siteDict[site].append(_['_id'])

        vs1 = set(siteDict['NTIS']) & set(siteDict['SCIENCEON'])
        vs2 = set(siteDict['DBPIA']) & set(siteDict['KCI'])
        vs3 = set(siteDict['SCOPUS']) & set(siteDict['WOS'])

        vsResult = list(vs1 & vs2 & vs3)
        vsResult.sort(reverse=True)

        return vsResult


# def get_parameter(keyList):
#     # print("keyList : ", keyList)
#     for key in keyList:
#         print("key : ", key)
#         ls = list(expertFactor.find(
#             {
#                 'keyId':key,
#                 "Productivity":{"$gt":0}, 
#             },
#             {
#                 "_id" : 0, 
#                 "keyId" : 1, 
#                 "A_ID" : 1, 
#                 "Productivity": 1, 
#                 "contirb" : 1, 
#                 "Durability" : 1, 
#                 "Recentness" : 1, 
#                 "Quality" : 1, 
#                 "Acc" : 1 
#             }))
#         # print(ls)
#         for _id in ls:
#             # print(_id)
#             __id = author.find_one({'_id':_id['A_ID']})
#             # print(__id)
#             if __id is not None and 'isCollect' not in __id:
#                 result = rawData.find_one({'keyId':key,'mngId':_id['A_ID']},{'keyId':1,'mng':1,'IdAgency':1,'koTitle':1,'enTitle':1, '_id':0})
#                 # print(result)
#                 # if result is not None:
#                     # print(result)
#                 yield result
#                     # return result

__main__()
