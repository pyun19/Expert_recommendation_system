from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
from multiprocessing.managers import BaseManager
from collections import OrderedDict
from multiprocessing import Process, Value, Array
import time
import csv
import pprint
import re
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
from json import dumps
from kafka import KafkaProducer
from pyjarowinkler import distance
csv.field_size_limit(sys.maxsize)

class Parser :
    '''
    @ Method Name     : __init__
    @ Method explain  : 파싱에 필요한 파라미터
    '''
    def __init__(self ,total_data, keyId, site, path, isPartial, is_not_active, chrome_failed, numProcess, _ip):
        self.total_data    = total_data
        self.keyId         = keyId
        self.site          = site
        self.path          = path
        self.cnt           = 0
        self.producer      = KafkaProducer(bootstrap_servers= _ip + ':9092', value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        self.isPartial     = isPartial
        self.temp          = {}
        self.paper_cnt     = 0
        self.error_count   = 0
        self.inst_error    = 0
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.numProcess    = numProcess

    '''
    @ Method Name     : SCOPUS_Parsing
    @ Method explain  : scopus 파싱 데이터 전처리 및 정제
    '''
    def SCOPUS_Parsing(self, filename, isLast, path):
        print("scopus parsing")
        PaperRawSchema = ['id', 'title', 'author','journal', 'issue_inst', 'issue_year', 'issue_lang',
                          'start_page', 'end_page', 'paper_keyword', 'abstract', 'author_inst', 'author_id', 'citation','Link']

        column_name = ['EID', 'Title', 'Authors', 'Publisher', 'Affiliations', 'Year', 'Language of Original Document',
        'Page start',	'Page end', 'Author Keywords', 'Abstract', 'Authors with affiliations', 'Author(s) ID', 'Cited by','Link']

        scopus_index = [36, 2, 0, 25, 14, 3, 30, 8, 9, 17, 16, 15, 1, 11, 13] #EID 34 -> 36

        tempRaw = {}
        # csv_path = (self.path + filename)
        csv_path = (path + filename)
        print(csv_path, "  :    엑세파일 경로 및 이름 출력")
        print("엑셀파일 불러오기")
        # cnt = 0
        global Column
        with open(csv_path, "r", encoding="UTF-8") as f:
            # raw_data = {'english_title'}
            csv_data = csv.reader(f)
            for column in csv_data:
                Column = column
                break
            Column[0] = 'Authors'
            column_idx = [Column.index(i) for i in column_name]                 # [12, 2, 0, 25, 14, 3, 30, 8, 9, 17, 16, 15, 1, 11]

            try:
                # next(csv_data)  # 2행부터 읽는다.

                raw_data = {}
                for row in csv_data:
                    try :
                        # row = csv_data[i]
                        # print("row:", row)
                        self.paper_cnt     += 1
                        raw_data = {'english_title' : '', 'english_abstract' : '' , 'isPartial' : 'False'}

                        try:
                            if 'No author id available' in row[scopus_index[PaperRawSchema.index('id')]] or 'No author name available' in row[scopus_index[PaperRawSchema.index('author')]] :
                                continue
                        except:
                            if 'No author id available' in row[33] or 'No author name available' in row[scopus_index[PaperRawSchema.index('author')]] :
                                continue

                        for i in range(len(PaperRawSchema)):
                            # raw_data[PaperRawSchema[i]] = row[scopus_index[i]]
                            raw_data[PaperRawSchema[i]] = row[column_idx[i]]

                        if not raw_data['title'][-1].isalnum():
                            raw_data['title']    = raw_data['title'][:-1]

                        if raw_data['citation'] == '' or raw_data['citation'] is None:
                            raw_data['citation'] = 0
                        raw_data['id']           = raw_data['id'].replace('2-s2.0-', '')
                        raw_data['author_id']    = raw_data['author_id'][0:len(raw_data['author_id'])-1]
                        raw_data['start_page']   = re.sub('\D', '', raw_data['start_page'])
                        raw_data['end_page']     = re.sub('\D', '', raw_data['end_page'])
                        raw_data['author']       = raw_data['author'].replace(',', ';')
                        author_info              = raw_data['author_inst'].replace(',', '').replace(';', ' ;').split(' ')
                        author_name              = raw_data['author'].replace(';', '').split(' ')


                        author_inst = []
                        for i in author_info:
                            if i not in author_name:
                                author_inst.append(i)
                        split_author_inst = ' '.join(author_inst).split(';')

                        for i in range(len(split_author_inst)):
                            if split_author_inst[i] == ' ':
                                # print(i, split_author_inst[i])
                                split_author_inst[i] = split_author_inst[i - 1]

                            elif split_author_inst[i] == '':
                                split_author_inst[i] = split_author_inst[i - 1][:-1]

                            else:
                                pass
                        raw_data['author_inst'] = '; '.join(split_author_inst).replace(' ;', ';')
                        raw_data['keyId'] = self.keyId

                        raw_data['isPartial'] = self.isPartial.value
                        td = self.total_data.value
                        self.cnt += 1
                        pro = self.cnt / td
                        if pro >= 1.0 :
                            # if isLast and i == (size-1) :
                            #     pro = 1.0
                            # else :
                            pro = 0.99
                        raw_data['progress'] = pro
                        #self.producer.send(self.site, value=raw_data)
                        if len(tempRaw) != 0 :
                            self.producer.send(self.site, value=tempRaw)

                        tempRaw = raw_data.copy()
                    except Exception as e :
                        print(e)
                        # print(row)
                        # pprint.pprint(raw_data)
                        self.error_count   += 1
                        print("====parsing ERROR====")
            except:
                print("===== empty file =====")
                self.isPartial.value = 1
        # pprint.pprint(tempRaw)
        print("====parsing SUCCESS====")
        print("")
        print("producer 보낸 논문수 : ", self.paper_cnt,'|| 총 논문수 : ', self.total_data.value)
        print('Exception 데이터 수 : ', self.error_count)
        print("")

        # print(self.producer)
        if len(tempRaw) != 0 :
            # if isLast == True :
            #     tempRaw['progress'] = 1.0
            self.producer.send(self.site, value=tempRaw)
            # pprint.pprint("producer send end")
            # pprint.pprint(tempRaw)
        # elif isLast == True:
        #     self.flush()

    '''
    @ Method Name     : flush
    @ Method explain  : kafka flush 실행, 진생 상황(progress, 크롬 상태, isPartial 체크)
    '''
    def flush(self) :
        numMessges = 0
        try :
            temp = {'progress' : 1, 'keyId' : self.keyId, 'ok' : 1, 'is_not_active' : self.is_not_active.value}
            if self.is_not_active.value == 1:
                temp['API LIMIT'] = -3
            if self.chrome_failed.value == self.numProcess:
                temp['fail'] = 1
            elif self.chrome_failed.value > 0:
                self.isPartial.value = 1

            temp['isPartial'] = self.isPartial.value
            print(temp)
            self.producer.send(self.site, value = temp)
            numMessges = self.producer.flush()

        except Exception as e :
            print(e)

        # print(numMessges)

    '''
    @ Method Name     : on_send_success
    @ Method explain  : kafka 데이터 보내기 성공 확인 프린트
    '''
    def on_send_success(self, record_metadata):
        print(record_metadata.topic)
        print(record_metadata.partition)
        print(record_metadata.offset)

    '''
    @ Method Name     : on_send_error
    @ Method explain  : kafka 데이터 보내기 에러 확인 프린트
    '''
    def on_send_error(self, excp):
        print(excp)
        # log.error('I am an errback', exc_info=excp)


    '''
    @ Method Name     : WOS_Parsing
    @ Method explain  : WOS 파싱 데이터 전처리 및 정제
    '''
    def WOS_Parsing(self, filename, isLast):
        try:

            webofscience_raw_schema = ['title', 'source title', 'journal', 'issue_inst', 'issue_lang', 'abstract']
            webofscience_index = [8, 9, 41, 35, 12, 21]

            #file_list = os.listdir(self.path)
            #file = file_list[0]
            file = filename
            print(file)
            excel_path = (self.path + '/' + file)
            print(excel_path, "  :    엑세파일 경로 및 이름 출력")
            wb = xlrd.open_workbook(excel_path)
            sh = wb.sheet_by_index(0)
            print("엑셀파일 불러오기")
            paper_cnt = 0
            for rownum in range(1, sh.nrows):
                try:
                    # raw_data = OrderedDict()
                    raw_data = {}
                    row_values = sh.row_values(rownum)

                    for i in range(len(webofscience_raw_schema)):
                        raw_data[str(webofscience_raw_schema[i])] = row_values[webofscience_index[i]]
                    raw_data['id'] = row_values[61].replace('WOS:', '')
                    raw_data['author'] = row_values[5]

                    if not row_values[22]:  # NULL 체크
                        raw_data['author_inst'] = ''

                    else:
                        if ('[' or ']') in row_values[22]:
                            start_index = []
                            end_index = []
                            target_a = '['
                            target_b = ']'
                            s_index = -1
                            e_index = -1
                            for i in row_values[22]:
                                s_index = row_values[22].find(target_a, s_index + 1)
                                e_index = row_values[22].find(target_b, e_index + 1)
                                if s_index == -1:
                                    break
                                elif e_index == -1:
                                    break
                                start_index.append(s_index)
                                end_index.append(e_index + 1)

                            author = row_values[5].replace(" ", '').split(';')
                            s_author_info = []
                            s_author_inst = []
                            for i in range(len(start_index)):
                                str_author_info = row_values[22][start_index[i]:end_index[i]]
                                if i == len(start_index) - 1:
                                    str_author_inst = row_values[22][end_index[i]:]
                                else:
                                    str_author_inst = row_values[22][end_index[i]:start_index[i + 1]]
                                    pass

                                str_author_info = str_author_info.replace('[', '').replace(']', '').replace(' ', '').split(';')
                                str_author_inst = str_author_inst.replace(';', '')
                                s_author_info.append(str_author_info)
                                s_author_inst.append(str_author_inst)

                                i = 0
                                temp = []
                                temp_a = []
                                author_final = []
                                for i in range(len(s_author_inst)):
                                    j = 0
                                    for j in range(len(author)):
                                        if author[j] in s_author_info[i]:
                                            if author[j] not in temp:
                                                temp.append(author[j])
                                                a = s_author_inst[i] + ';'
                                                author_final.append(a)
                                            else:
                                                pass
                                            pass
                                        else:
                                            pass

                            temp_x = ''
                            for i in range(len(author_final)):
                                temp_x += author_final[i]
                            raw_data['author_inst'] = temp_x[:-1]

                        else:
                            raw_data['author_inst'] = row_values[22]

                    # raw_data['author_inst'] = str(author_final).replace('[', '').replace(']', '').replace("'", "")[:-1]

                    if not raw_data['author'].count(';') == raw_data['author_inst'].count(';'):
                        self.inst_error += 1
                        continue
                    else:
                        pass


                    #Researcher Ids (WOS) & ORCIDs (US)
                    author = row_values[5]
                    author_list = author.split('; ')
                    Research_id = row_values[25]
                    Research_id_list = Research_id.split('; ')
                    ORCID_id = row_values[26]
                    ORCID_id_list = ORCID_id.split('; ')
                    R_id = ""
                    O_id = ""

                    # for i in range(len(author_list)):
                    #     if Research_id:
                    #         for j in range(len(Research_id_list)):
                    #             if re.search(Research_id_list[j][:-12].upper(), author_list[i].upper()) or re.search(author_list[i].upper(), Research_id_list[j].upper()):
                    #                 R_id += Research_id_list[j][-12:] + ';'
                    #             R_id += ";"
                    #     if ORCID_id:
                    #         for j in range(len(ORCID_id_list)):
                    #             if ORCID_id == "":
                    #                 O_id += ";"
                    #             elif re.search(ORCID_id_list[j][:-20].upper(), author_list[i].upper()) or re.search(author_list[i].upper(), ORCID_id_list[j].upper()):
                    #                 O_id +=ORCID_id_list[j][-20:] + ';'

                    for i in range(len(author_list)):
                        if Research_id:
                            for j in range(len(Research_id_list)):
                                if Research_id_list[j][:-12].upper() == author_list[i].upper():
                                    R_id += Research_id_list[j][-11:]
                                R_id += ";"
                        if ORCID_id:
                            for l in range(len(ORCID_id_list)):
                                a = re.sub(';','', ORCID_id_list[l])
                                if a[:-20].upper() == author_list[i].upper():
                                    O_id += ORCID_id_list[l][-19:]
                                O_id += ";"

                    if R_id:
                        raw_data['R_id'] = R_id
                    else:
                        raw_data['R_id'] = ';'*raw_data['author'].count(';')
                    if O_id:
                        raw_data['O_id'] = O_id
                    else:
                        raw_data['O_id'] = ';'*raw_data['author'].count(';')


                    if row_values[44]:
                        raw_data['issue_year'] = int(row_values[44])
                    else:
                        if str(row_values[56]).isdigit() == False:
                            raw_data['issue_year'] = row_values[56]
                            raw_data['issue_year'] = int(re.sub('\D', '', raw_data['issue_year']))
                        else:
                            raw_data['issue_year'] = int(row_values[56])

                    if row_values[51]:
                        raw_data['start_page'] = re.sub('[-=+,#/\?:^$.@*※~&%ㆍ!』‘|\(\)\[\]\<\>`…》]', '', str(row_values[51]))
                        if raw_data['start_page']:
                            if str(row_values[51]).isdigit() == False:
                                raw_data['start_page'] = int(re.sub('\D', '', raw_data['start_page']))
                            else:
                                raw_data['start_page'] = int(row_values[51])
                        else:
                            raw_data['start_page'] = 0
                    else:
                        raw_data['start_page'] = 0

                    if row_values[52]:
                        raw_data['end_page'] = re.sub('[-=+,#/\?:^$.@*※~&%ㆍ!』‘|\(\)\[\]\<\>`…》]', '', str(row_values[52]))
                        if raw_data['end_page']:
                            if str(row_values[52]).isdigit() == False:
                                raw_data['end_page'] = int(re.sub('\D', '', raw_data['end_page']))
                            else:
                                raw_data['end_page'] = int(row_values[52])
                        else:
                            raw_data['end_page'] = 0
                    else:
                        raw_data['end_page'] = 0

                    if row_values[30]:
                        raw_data['citation'] = int(row_values[30])
                    else:
                        raw_data['citation'] = 0

                    if row_values[34]:
                        raw_data['Usage Count'] = int(row_values[34])
                    else:
                        raw_data['Usage Count'] = 1

                    if row_values[19]:
                        raw_data['author_keyword'] = row_values[19]
                    else:
                        raw_data['author_keyword'] = ''

                    if row_values[59] or row_values[20]:
                        raw_data['paper_keyword'] = row_values[59] or row_values[20]
                    else:
                        raw_data['paper_keyword'] = ''

                    #raw_data['author_id'] = ''
                    raw_data['english_title'] = ''
                    raw_data['english_abstract'] = ''
                    raw_data['keyId'] = self.keyId

                    self.paper_cnt += 1
                    raw_data['progress'] = self.paper_cnt / self.total_data.value # 다운로드한 논문수 == cnt(변수 재설정) / 총 논문수(처음 검색할때)
                    if raw_data['progress'] >= 1:
                        raw_data['progress'] = 0.9999

                    raw_data['isPartial'] = self.isPartial.value
                    raw_data['is_not_active'] = self.is_not_active.value

                    # pprint.pprint(raw_data, indent=8)
                    # raw_data = raw_data.__dict__
                    if len(self.temp) != 0 :
                        self.producer.send(self.site, value = self.temp)
                        # .add_callback(self.on_send_success).add_errback(self.on_send_error)
                        # self.producer.send(self.site, value={"Data" : 1})
                        self.producer.flush()
                        # print(type(temp2))
                        # print('producer temp')
                    self.temp = raw_data.copy()
                    #pprint.pprint(self.temp, indent=8)

                except:
                    print(rownum, ' 번째 예외 데이터 존재')
                    # pprint.pprint(raw_data)
                    print("====parsing ERROR====")
                    self.error_count += 1
                    pass


            if len(self.temp) != 0 :
                self.producer.send(self.site, value = self.temp)
                # .add_callback(self.on_send_success).add_errback(self.on_send_error)
                print('producer temp end')
#                pprint.pprint(self.temp, indent=8)

            self.temp = {}
            print("producer 보낸 논문수 : ", self.paper_cnt,'|| 총 논문수 : ', self.total_data.value)
            print('Exception 데이터 수 : ', self.error_count, 'Inst error : ', self.inst_error)

        except:
            self.isPartial.value = 1
            pass

    '''
    @ Method Name     : parsing
    @ Method explain  : 사이트별 파싱 데아터 전처리 및 정제 실행
    '''
    def parsing(self, filename, isLast, path):

        if self.site == 'SCOPUS' :
            self.SCOPUS_Parsing(filename, isLast, path)
        elif self.site == 'WOS':
            self.WOS_Parsing(filename, isLast)
        else :
            print('Site 입력 에러')
            Exception

class FileUpdateHandler(FileSystemEventHandler) :
    '''
    @ Method Name     : __init__
    @ Method explain  : 왓치독 실행에 필요한 파라미터
    '''
    def __init__(self, crawl_end, parse_end, num_data, num_parse, total_data, keyId, site, path, isPartial, numProcess, is_not_active, chrome_failed, _ip) :
        super(FileUpdateHandler, self).__init__()
        #print(path)
        self.crawl_end = crawl_end
        self.parse_end = parse_end
        self.num_data  = num_data
        self.num_parse = num_parse
        self.path = path
        self.isPartial = isPartial
        self.site = site
        #print(self.path)
        self.isLast = False
        self.numProcess = numProcess
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.parser = Parser(total_data, keyId, site, self.path, self.isPartial, self.is_not_active, self.chrome_failed, self.numProcess, _ip)
        # QueueManager.register('observerQ')
        # m = QueueManager(address=('localhost', 50001), authkey=b'dojin')
        # m.connect()
        # self.observerQ = m.get_queue()

    def on_any_event(self, event) :
        pass
    '''
    @ Method Name     : on_created
    @ Method explain  : 파일 생성 감지로 종료(크롬이 종료하면 파일생성됨)
    '''
    def on_created(self, event):
        fileName = event.src_path
        if "crawler_end" in fileName:
            print('Crawl_end ===> 종료 파일 생성이 감지 되었습니다.')
            self.isLast = True
            if self.num_parse.value == self.num_data.value:
                print('waiting parse')
                self.parse_end.value = 1
            else:
                if self.site == 'SCOPUS':
                    self.parse_end.value = 1
                else:
                    self.crawl_end.value = 1
        else:
            pass

    '''
    @ Method Name     : on_deleted
    @ Method explain  : 파일 삭제 감지로 최종 종료(모든 프로세스(크롬)가 종료하면 파일생성됨)
    '''
    def on_deleted(self, event):

        fileName = event.src_path
        if self.parse_end.value != 2:
            print(fileName)
            print('Crawl_end ===> 삭제가 감지 되었습니다. progress = 1 을 보냅니다.')
            self.isLast = True
            #self.parser.parsing(fileName, self.isLast)
            self.parser.flush()
            print("flush end")
            self.parse_end.value = 2


    def on_moved(self, event):
        pass

    '''
    @ Method Name     : on_modified
    @ Method explain  : 파싱 저장 경로에 파일이 생성 되면 파싱 실행
    '''
    def on_modified(self, event) :
        if not event.is_directory :
            fileName = event.src_path.split("/")[-1]
            path = event.src_path.replace(fileName,"")
            if "crdownload" not in fileName and "crawler_end" not in fileName:
                if self.num_data.value > self.num_parse.value:
                    self.num_parse.value += 1
                    self.parser.parsing(fileName, self.isLast, path)
                if self.crawl_end.value == self.numProcess:
                    print("crawl ended")
                    if self.num_data.value == self.num_parse.value :    #다운받은 파일 수와 파싱된 파일의 수가 같으면
                        print("parse ended")
                        self.isLast = True

                if self.isLast == True :
                    self.parse_end.value = 1

                print(fileName)
                print("parse", self.num_parse.value)


class FileObserver :
    '''
    @ Method Name     : __init__
    @ Method explain  : 파일 옵저버(지정 경로)에 필요한 파라미터
    '''
    def __init__(self, _path, crawl_end, parse_end, parse_data, num_data, total_data, keyId, site, isPartial, numProcess, is_not_active, chrome_failed, _ip):
        self.crawl_end  = crawl_end
        self.parse_end  = parse_end
        self.num_data   = num_data
        self.total_data = total_data
        # self.producer   = producer
        self.keyId      = keyId
        self.site       = site
        self.path       = _path
        self.num_parse  = parse_data
        self.isPartial  = isPartial
        self.numProcess = numProcess
        self.is_not_active = is_not_active
        self.chrome_failed = chrome_failed
        self.IP = _ip
        print(self.path)

    '''
    @ Method Name     : run
    @ Method explain  : 파일 옵저버(지정 경로) 이벤트 감지 실행
    '''
    def run(self) :
        event_handler = FileUpdateHandler(self.crawl_end, self.parse_end, self.num_data, self.num_parse, self.total_data, self.keyId, self.site, self.path, self.isPartial, self.numProcess, self.is_not_active, self.chrome_failed, self.IP)
        observer = Observer()
        observer.schedule(event_handler, self.path, recursive=True)     #recursive : 하위 디렉토리 event 감지포함
        observer.start()
        cnt = 0
        parse_error = False
        try:
            while (self.crawl_end.value != self.numProcess) or (self.parse_end.value != 2):
                time.sleep(5)
                cnt += 1
                if cnt == 3:
                    print("keyId:", self.keyId, "observer stop", self.crawl_end.value, "Parse stop : ", self.parse_end.value, "Data count : ", self.num_data.value, "Parse count : ", self.num_parse.value)
                    cnt = 0

        except KeyboardInterrupt:
            observer.stop()

        observer.stop()
        observer.join()
        print("observer end")
