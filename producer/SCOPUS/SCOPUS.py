from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from collections import OrderedDict
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
from selenium.webdriver.support.wait import WebDriverWait
from watchdog.observers import Observer
from watchdog.events import LoggingEventHandler
from watchdog.events import FileSystemEventHandler
from multiprocessing.managers import BaseManager
from multiprocessing import Process, Value, Array
import time
import re
import csv
import threading
import logging

class SCOPUS_crawler:
    '''
        f_query         : 첫 검색 시 입력 query / format: TITLE-ABS-KEY ( keyword )  AND  ( LIMIT-TO ( PUBYEAR ,  Year ) )
        query           : 기본 query            / format: TITLE-ABS-KEY ( keyword )
        p               : process NUMBER
        num_data        : Number of downloaded files
        KeyId           : 검색 KeyId
        isPartial       : 부분 수집 flag        / True: 부분 수집, False: 전체 수집, default: False
        chrome_failed   : chrome open fail flag / True: open fail, False: open success, default: False
        korea_option    : only korea flag       / 0: Only korea, 1:All countries
    '''
    def __init__(self, f_query, query, p, num_data, keyId, isPartial, chrome_failed, korea_option, url, etc_path,project_path):
        self.chrome_options = webdriver.ChromeOptions()
        self.chrome_options.add_argument('--headless')
        self.chrome_options.add_argument('--no-sandbox')
        self.chrome_options.add_argument("--disable-gpu")
        self.chrome_options.add_argument('--disable-dev-shm-usage')
        self.chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows Phone 10.0; Android 4.2.1; Microsoft; Lumia 640 XL LTE) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Mobile Safari/537.36 Edge/12.10166')
        self.keyId = keyId
        self.p = p
        self.chrome_options.add_experimental_option("prefs", {
        'download.default_directory' : project_path+"/producer/Crawled_file/SCOPUS/" + str(self.keyId) + "/" +str(p),
        'profile.default_content_setting_values.automatic_downloads': 1})
        self.driver = webdriver.Chrome(etc_path + "/chromedriver", chrome_options=self.chrome_options)
        self.driver.implicitly_wait(2)
        self.total_amount  = 0
        self.download_count = num_data
        self.popup = False
        self.popup2 = False
        self.end = False
        self.country_flag = False
        self.keyword_flag = False
        self.document_flag = False
        self.csv_flag = False
        self.crawl_error = False
        self.papers_per_year = {}
        self.papers_per_language = {}
        self.papers_per_country = {}
        self.papers_per_country_2 = {}
        self.papers_per_keyword = {}
        self.papers_per_DocumentType = {}
        self.over_2000_papers_county = []
        self.over_2000_papers_keyword = []
        self.chrome_failed = chrome_failed
        self.sleepTime = 0.05
        self.timeOut = 2
        self.f_query = f_query
        self.query = query
        self.isPartial = isPartial
        self.korea_option = korea_option
        self.url = url


        self.logger = logging.getLogger('SCOPUS_log')
        self.logger.setLevel(logging.INFO)

        self.Formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        self.File = logging.FileHandler(filename=str(self.keyId)+"log.log")
        self.File.setFormatter(self.Formatter)
        self.File.setLevel(logging.INFO)

        self.logger.addHandler(self.File)
#================================================================================

    def action(self, flag, _param, flag2, _param2):
        '''
            flag    : find_element_by_"flag"    / 1: id, 2: css_selector, 3:class_name, 4: x_path
            _param  : flag 속성값
            flag2   : action                    / 1: click(), 2:send_keys, 3:text가져오기 4: clear
            _param2 : flag2 2번 send_keys query
        '''
        if self.popup == False:
            try:
                time.sleep(1)
                close = self.driver.find_element_by_class_name("_pendo-close-guide").click()
                self.logger.info(f"[{self.p}] 팝업창 닫기 성공")
                self.popup = True
            except:
                try:
                    close = self.driver.find_element_by_class_name("_pendo-close-guide_").click()
                    self.logger.info(f"[{self.p}] 팝업창 닫기 성공")
                    self.popup = True
                except:
                    pass

        if self.popup2 == False:
            try:
                time.sleep(1)
                if "_pendo-step-container-styles" == self.driver.find_element_by_css_selector("#pendo-guide-container").get_attribute('class'):
                    close = self.driver.find_element_by_class_name("_pendo-close-guide").click()
            except:
                try:
                    close = self.driver.find_element_by_css_selector("#pendo-close-guide-a8eb41e7").click()
                    self.popup2 = True
                except:
                    pass

        if flag == 1:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_id(_param))
        elif flag == 2:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_css_selector(_param))
        elif flag == 3:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_class_name(_param))
        elif flag == 4:
            temp = WebDriverWait(self.driver, self.timeOut).until(lambda x: x.find_element_by_xpath(_param))

        if flag2 == 1:
            temp.click()
        elif flag2 == 2:
            temp.send_keys(_param2)
            temp.send_keys(Keys.ENTER)
        elif flag2 == 3:
            return temp.text
        elif flag2 == 4:
            return temp.clear()

    def HTML(self, _tag):
        '''
            _tag: html _tag 속성
        '''
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        return soup.select(_tag)

    def checkModalOpen(self, _id):
        '''
            해당 css_selector(_id) 요소 class속성에 'in'이 있을때 까지 wait  --> viewAll loading 완료 기다리
        '''
        while True:
            if 'in' in self.driver.find_element_by_css_selector(_id).get_attribute('class').split():
                break
            time.sleep(self.sleepTime)

    def open_site(self, query):
        self.driver.get(self.url)
        time.sleep(1)
        # while 1:
            # try:
        #         try:
        #             if 'scopus-homepage' in self.driver.find_element_by_id("scopus-homepage").get_attribute('id').split():
        #                 self.logger.info(f"[{self.p}] New-homepage 접근")
        #                 print("New-Homepage")
        #                 try:
        #                     self.action(2, "#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a > span.link__text", 1, None)
        #                 except:
        #                     self.action(4, "//*[@id="documents-tab-panel"]/div/micro-ui/scopus-document-search-form/form/div[2]/div[1]/a/span[1]", 1, None)
        #             else:
        #                 self.logger.info(f"[{self.p}] ex-honmepage 접근")
        #                 print("Ex-Homepage")
        #         except:
        #             self.action(2, "#advSearchLink > span", 1, None)
        self.action(1, "searchfield", 2, query)
        #         break
        #     except:
        #         time.sleep(5)
        # self.logger.info(f"[{self.p}] query 입력 성공")
        print(self.p, "검색 성공")
        return True

    def re_search(self, re_query):
        try:
            self.action(1, 'editAuthSearch', 1, None)
        except :
            try:
                self.action(2, '# editAuthSearch', 1, None)
            except:
                try:
                    self.logger.info(f"[{self.p}] warning: edit click 실패 --> URL재검색[ERROR]")
                    self.driver.get(self.url)
                    if 'scopus-homepage' in self.driver.find_element_by_id("scopus-homepage").get_attribute('id').split():
                        self.action(2, "#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a > span.link__text", 1, None)
                    else:
                        self.action(2, "#advSearchLink > span", 1, None)
                except:
                    self.logger.info(f"[{self.p}] re_search 실패(Line 190) --> 프로세스 종료[ERROR]")
                    print("!!!!!!!!!!!!!! Crawl_error !!!!!!!!!!!!!!")
                    self.crawl_error = True
                    return 0
        # Advaced 접근 완료

        self.action(1, 'searchfield', 4, None)  # 검색창 초기화
        self.action(1, "searchfield", 2, re_query)
        print("")
        print("===============================================================================================================")
        print(self.p, re_query)
        self.logger.info(f"[{self.p}]: query 입력 성공")
        print("===============================================================================================================")
        print("")

        return self.total_count(re_query)

    def total_count(self, re_query):
        retry = 3
        while retry:
            try:#searchResFormId > div:nth-child(2) > div > header > h1 > span.resultsCount
                #_param = "#searchResFormId > div:nth-child(2) > div > header > h1 > "
                try:
                    _param = "#searchResFormId > div:nth-child(3) > div > header > h1 > span.resultsCount"
                except:
                    _param = "#searchResFormId > div:nth-child(2) > div > header > h1 > span.resultsCount"
                temp = WebDriverWait(self.driver, 10).until(lambda x: x.find_element_by_css_selector(_param))
                total_papers = temp.text
                if total_papers != '':
                    break
                retry -= 1
                time.sleep(5)
            except:
                try:
                    self.logger.info(f"[{self.p}] warning: 논문수 가져오기 실패 ----> 새로고침 retry: {retry}[ERROR]")
                    print("논문수 가져오기 실패 ----> 새로고침")
                    self.driver.get(self.url)
                    time.sleep(1)
                    if 'scopus-homepage' in self.driver.find_element_by_id("scopus-homepage").get_attribute(
                            'id').split():
                        self.action(2,
                                    "#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a > span.link__text",
                                    1, None)
                    else:
                        self.action(2, "#advSearchLink > span", 1, None)
                    self.action(1, 'searchfield', 4, None)
                    self.action(1, "searchfield", 2, re_query)
                except Exception as e:
                    self.logger.info(f"[{self.p}] warning: 논문수 가져오기 실패 ----> process 종료[ERROR]")
                    self.crawl_error = True
                    return 0



#//*[@id="documents-tab-panel"]/div/micro-ui/scopus-document-search-form/form/div[2]/div[1]/a/span[1]
#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a > span.link__text
#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a > span.link__text
#documents-tab-panel > div > micro-ui > scopus-document-search-form > form > div.DocumentSearchForm__flex___2Tssv.DocumentSearchForm__alignItemsCenter___2qVeR.DocumentSearchForm__justifyContentSpaceBetween___18o8k.margin-size-16-t > div:nth-child(1) > a

        if 'Document search results' in total_papers:
            self.total_amount = 0
        else:
            total_papers = total_papers.split(' ')[0]
            self.total_amount = int(re.sub(',', '', total_papers))

        self.logger.info(f"[{self.p}] 총 논문 수: {self.total_amount}")
        print(self.p, "총 결과 수:", self.total_amount)
        return self.total_amount


    def years(self):
        try:
            self.action(4, '//*[@id="documents-tab-panel"]/div/micro-ui/scopus-document-search-form/form/div[2]/div[1]/a/span[1]', 1, None)
            self.action(2, "#viewAllLink_PUBYEAR", 1, None)
            self.checkModalOpen('#navigatorOverlay_PUBYEAR')

            row_years = self.HTML('div.row.body > ul > li > label.checkbox-label > span.btnText')
            row_papers = self.HTML('div.row.body > ul > li > button > span.badge > span.btnText')
            self.action(2, "#resultViewMoreModalMainContent_PUBYEAR > div.modal-header > button", 1, None)
        except:
            row_years = self.HTML('#cluster_PUBYEAR > li.checkbox > label.checkbox-label > span.btnText')
            row_papers = self.HTML('#cluster_PUBYEAR > li > button > span.badge > span.btnText')

        for i, j in zip(row_years, row_papers):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            self.papers_per_year[i] = int(j)

    def creat_query(self, item, values, limit):
        new_query = ''
        arr = []
        if limit == True:
            for i in values:
                temp = ' LIMIT-TO ' + '( ' + item + ',' + '\"' + i + '\"' + ')'
                arr.append(temp)
        else:
            for i in values:
                temp = ' EXCLUDE ' + '( ' + item + ',' + '\"' + i[0] + '\"' + ')'
                arr.append(temp)

        string = ' OR '.join(arr)
        new_query = ' AND (' + string + ' )'
        return new_query

    def download(self, count, isPartial):
        if count <= 2000:
            try:
                #self.action(2, '#selectAllCheck', 1, None)
                temp = self.driver.find_elements_by_css_selector("#selectAllCheck")
                temp[0].click()
                self.action(1, 'export_results', 1, None)
                self.action(2, '#exportList > li:nth-child(5) > label', 1, None)
                self.check_checkbox(count, isPartial)
            except:
                return
        else:
            self.logger.info(f"[{self.p}] 논문수 2000개 초과 --> 다운로드 진행X")
            print("2000개 초과로 다운로드 진행 X")
            self.isPartial = 1
            return

    def check_checkbox(self, count, isPartial):
        if self.csv_flag == False:
            self.action(2, '#bibliographicalInformationCheckboxes > span > label', 1, None)
            self.action(2, '#abstractInformationCheckboxes > span > label', 1, None)
            self.action(2, '#fundInformationCheckboxes > span > label', 1, None)
            self.csv_flag = True

        self.action(2, '#exportTrigger', 1, None)
        self.download_count.value += 1
        self.logger.info(f"[{self.p}] 다운로드 total_downloaded file: {self.download_count.value}")
        print(self.p, "다운로드:", self.download_count.value)

    def get_attributePaper(self, collapse, viewMore, viewAll, Modal, Item, Paper, close, temp_dict, flag, check):
        while 1:
            temp = self.driver.find_element_by_css_selector(check).get_attribute('class')
            if 'in' in temp:
                break
            else:
                self.action(2, collapse, 1, None)

        if flag == False:
            self.action(2, viewMore, 1, None)
        while 1:
            try:
                self.action(2, viewAll, 1, None)
                break
            except:
                self.driver.refresh()

        self.checkModalOpen(Modal)

        row_Item = self.HTML(Item)
        row_cnt = self.HTML(Paper)
        self.action(2, close, 1, None)

        for i, j in zip(row_Item, row_cnt):
            i = re.sub('\n', "", i.text)
            j = re.sub(',', "", j.text)
            temp_dict[i] = int(j)
        return temp_dict

    def under2000_years(self):
        year_papers = []
        for year in self.papers_per_year:
            year_papers.append([year, self.papers_per_year[year]])
        year_papers.sort(key=lambda x: x[1])

        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if year_papers[start_idx][1] > 2000:  # 2000개 이하의 논문수 연도가 없으면 끝남
                self.over_2000_papers = year_papers[start_idx:]
                break

            for idx in range(start_idx, len(year_papers)):
                cnt = cnt + year_papers[idx][1]
                if cnt > 2000:
                    end_idx = idx
                    Temp = year_papers[start_idx:end_idx]
                    val = ([i[0] for i in Temp])  # 2000이하 연도 리스트
                    re_query = self.query + self.creat_query('PUBYEAR', val, True)
                    count = self.re_search(re_query)
                    if count != 0:
                        self.download(count, self.isPartial)
                        year_papers = year_papers[end_idx:]
                    break
            if cnt < 2000 and cnt != 0:
                val = (i[0] for i in year_papers)
                re_query = self.query + self.creat_query('PUBYEAR', val, True)
                count = self.re_search(re_query)
                if count != 0:
                    self.download(count, self.isPartial)
                    self.over_2000_papers = []
                break

    def Access_type(self, ex_query, count):
        html = self.driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        All_open_Access = self.action(2, "#li_all > button > span.badge > span.btnText", 3, None)
        All_open_Access = int(re.sub(',', "", All_open_Access))
        other = count - All_open_Access

        if All_open_Access <= 2000 and other <= 2000:  # 둘 다 2000개 이하일 때,
            re_query = ex_query + self.creat_query('freetoread', ['all'], True)
            count = self.re_search(re_query)
            if count != 0:
                self.download(count, self.isPartial)

            re_query = ex_query + self.creat_query('freetoread', [['all']], False)
            count = self.re_search(re_query)
            if count != 0:
                self.download(count, self.isPartial)
            return True, 0, 0

        elif All_open_Access <= 2000:  # open_Acccess만 이하인 경우
            re_query = ex_query + self.creat_query('freetoread', ['all'], True)
            count = self.re_search(re_query)
            if count != 0:
                self.download(count, self.isPartial)
            return ex_query + self.creat_query('freetoread', [['all']], False), 0, other

        elif other <= 2000:
            re_query = ex_query + self.creat_query('freetoread', [['all']], False)
            count = self.re_search(re_query)
            if count != 0:
                self.download(count, self.isPartial)
            return ex_query + self.creat_query('freetoread', ['all'], True), All_open_Access, 0
        else:
            return ex_query, All_open_Access, other

    def search_Access(self, year, type, typeCnt, query):
        query_access = year + type
        self.re_search(query_access)
        self.search_country(query_access, typeCnt)  # country를 exclude로 재검색
        self.country_flag = True
        for idx in range(len(self.over_2000_papers_country)):
            query_country = query_access + query[1] + self.over_2000_papers_country[idx][0] + ') ) '
            self.re_search(query_country)
            self.search_keyword(query_country, self.over_2000_papers_country[idx][1])
            self.keyword_flag = True

    def search_country(self, query_access, Access_num):
        check = "#body_COUNTRY_NAME"
        Tag = "#collapse_COUNTRY_NAME_link"
        More_Tag = "#viewMoreLink_COUNTRY_NAME > span"
        All_Tag = "#viewAllLink_COUNTRY_NAME > span"
        Modal = "#navigatorOverlay_COUNTRY_NAME"
        Countrys = "div.row.body > ul > li > label.checkbox-label > span.btnText"
        Papers = "div.row.body > ul > li > button > span.badge > span.btnText"
        Close = "#resultViewMoreModalMainContent_COUNTRY_NAME > div.modal-header > button"
        self.papers_per_country = self.get_attributePaper(Tag, More_Tag, All_Tag, Modal, Countrys, Papers, Close,
                                                          self.papers_per_country, self.country_flag, check)

        country_papers = []
        for country in self.papers_per_country:
            if country == 'South Korea' and self.korea_option == 0:             # Only South Korea 검색
                continue                                                        # South Korea가 아니면 append X --> for문 종료)
            country_papers.append([country, self.papers_per_country[country]])  # country_papers = [["south Korea", 643],["China", 432]]

        country_papers.sort(key=lambda x: x[1], reverse=True)  # 내림차순 정렬
        exclude_country = []

        count = 0
        idx = 0
        temp_flag = True
        while idx < len(country_papers):
            exclude_country.append(country_papers[idx])
            if temp_flag == True:  # 처음에는 True
                Access_num = Access_num - country_papers[idx][1]  # 큰 수부터 빼기
            elif temp_flag == False:
                count -= country_papers[idx][1]
            if Access_num <= 2000 and count <= 2000:  # Access_num이 2000이하가 되고 count가 2000이하면(처음에는 무조건 2000이하)
                a = self.creat_query('AFFILCOUNTRY', exclude_country, False)  # Exclude country 쿼리 생성
                re_query = query_access + a
                count = self.re_search(re_query)  # 재검색
                if count <= 2000:  # 2000개 이하가 되면 while 탈출 --> 다운로드 진행(376 line)
                    break
                temp_flag = False  # 2000개 넘으면 temp_flag = False --> idx +1 다시 while문 시작 (360 line)
            idx += 1

        if (count > 0) and (count <= 2000):
            self.download(count, self.isPartial)
            exclude_country.sort(key=lambda x: x[1])  # Exclude한 Country 오름차순으로 정렬
        elif count == 0:
            return  # 논문 수 가져오기 에러발생 --> 함수 종료

        while count != 0:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if exclude_country[start_idx][1] > 2000:  # exclude_country에 2000개 이하 논문수 나라가 없으면 함수 종료
                self.over_2000_papers_country = exclude_country  # self.over_2000_papers_country = exclude_country
                return

            if cnt <= 2000 and sum(i[1] for i in exclude_country) <= 2000:  # cnt가 2000이하 이며 배열의 모든 논문수 합이 2000이하면
                val = [i[0] for i in exclude_country]  # val = [나라이름 모음]
                re_query = query_access + self.creat_query('AFFILCOUNTRY', val, True)
                count = self.re_search(re_query)
                if count != 0:
                    self.download(count, self.isPartial)
                    self.over_2000_papers_country = []
                return

            if len(exclude_country) != 1 and sum(i[1] for i in exclude_country) > 2000:  # 나라는 여러개, 합이 2000이상이면
                for idx in range(start_idx, len(exclude_country)):
                    cnt = cnt + exclude_country[idx][1]  # 차례로 논문 수 합
                    if cnt > 2000:  # 2000개 넘으면
                        end_idx = idx  # idx 저장
                        Temp = exclude_country[start_idx:end_idx]  # start idx 부터 end idx까지 temp 저장
                        val = [i[0] for i in Temp]  # 2000이하 연도 리스트               # val = [나라이름 모음]
                        re_query = query_access + self.creat_query('AFFILCOUNTRY', val, True)  # Limit to로 쿼리 생성
                        count = self.re_search(re_query)
                        if count != 0:  # 재검색
                            self.download(count, self.isPartial)  # 다운로드
                            exclude_country = exclude_country[end_idx:]  # 검색한 나라 제외
                        break  # for문 out --> while문으로 되돌아감(398 line)

    def search_keyword(self, query_country, Access_num):
        check = "#body_EXACTKEYWORD"
        Tag = "#collapse_EXACTKEYWORD_link"
        More_Tag = "#viewMoreLink_EXACTKEYWORD > span"
        All_Tag = "#viewAllLink_EXACTKEYWORD > span"
        Modal = "#navigatorOverlay_EXACTKEYWORD"
        Keywords = "div.row.body > ul > li > label.checkbox-label > span.btnText"
        Papers = "div.row.body > ul > li > button > span.badge > span.btnText"
        Close = "#resultViewMoreModalMainContent_EXACTKEYWORD > div.modal-header > button"

        self.papers_per_keyword = self.get_attributePaper(Tag, More_Tag, All_Tag, Modal, Keywords, Papers, Close,
                                                          self.papers_per_keyword, self.keyword_flag, check)
        self.keyword_flag = True
        keyword_papers = []
        for keyword in self.papers_per_keyword:
            keyword_papers.append([keyword, self.papers_per_keyword[keyword]])

        keyword_papers.sort(key=lambda x: x[1], reverse=True)
        exclude_Keyword = []
        count = 0
        idx = 0
        retry = 3
        temp_flag = True
        while idx < len(keyword_papers):
            exclude_Keyword.append(keyword_papers[idx])
            if temp_flag == True:  # 처음에는 True
                Access_num -= keyword_papers[idx][1]  # 큰 수부터 빼기
            elif temp_flag == False:
                count -= keyword_papers[idx][1]

            if Access_num <= 2000 and count <= 2000:  # Access_num이 2000이하가 되고 count가 2000이하면(처음에는 무조건 2000이하)
                a = self.creat_query('EXACTKEYWORD', exclude_Keyword, False)  # Exclude country 쿼리 생성
                re_query = query_country + a
                count = self.re_search(re_query)  # 재검색
                if (count <= 2000) or (retry == 0) or count == 0:  # 2000개 이하가 되면 while 탈출 --> 다운로드 진행(452 line)
                    break  # 재검색이 3번넘으면 종료 --> (키워드 무한루프 발생) --> (455 line)
                temp_flag = False  # 2000개 넘으면 temp_flag = False --> idx +1 다시 while문 시작 (435 line)
            idx += 1
            retry -= 1

        if count > 0 and count <= 2000:
            self.download(count, self.isPartial)
            exclude_Keyword.sort(key=lambda x: x[1])

        elif count == 0:  # 논문수 가져오기 에러발생 -- 함수종료
            return

        else:
            self.search_Publication(re_query)  # 키워드로 못자름..... ==> Publication stage로 자름

        while 1:
            cnt = 0
            end_idx = 0
            start_idx = 0
            if exclude_Keyword[0][1] > 2000:  # 2000개 이하의 논문수 키워드 없으면 끝남
                self.over_2000_papers_keyword = exclude_Keyword[start_idx:]
                print("키워드 2000개 초과 ===> 종료")  # 더이상 자르기 귀찮음.....누군가 하겠지....화이팅..!
                return

            if cnt <= 2000 and sum(i[1] for i in exclude_Keyword) <= 2000:
                val = [i[0] for i in exclude_Keyword]
                re_query = query_country + self.creat_query('EXACTKEYWORD', val, True)
                count = self.re_search(re_query)
                self.download(count, self.isPartial)
                self.over_2000_papers_keyword = []
                return

            if len(exclude_Keyword) != 1 and sum(i[1] for i in exclude_Keyword) > 2000:
                for idx in range(start_idx, len(exclude_Keyword)):
                    cnt = cnt + exclude_Keyword[idx][1]
                    if cnt > 2000:
                        end_idx = idx
                        Temp = exclude_Keyword[start_idx:end_idx]
                        val = [i[0] for i in Temp]  # 2000이하 키워드 리스트
                        re_query = query_country + self.creat_query('EXACTKEYWORD', val, True)
                        count = self.re_search(re_query)
                        self.download(count, self.isPartial)
                        exclude_Keyword = exclude_Keyword[end_idx:]
                        break

    def search_Publication(self, re_query):
        self.action(1, "collapse_PUBSTAGE_link", 1, None)
        final = self.action(2, "#li_final > button > span.badge > span.btnText", 3, None)
        article = self.action(2, "#li_aip > button > span.badge > span.btnText", 3, None)
        final = int(re.sub(',', "", final))
        article = int(re.sub(',', "", article))

        if final <= 2000 and article <= 2000:  # 둘다 2000개 이하면 LIMITO 이용해서 각각 다운로드 진행
            final_query = re_query + " AND  ( LIMIT-TO ( PUBSTAGE , \"final\" ) ) "
            self.re_search(final_query)
            self.download(final, self.isPartial)

            article_query = re_query + " AND  ( LIMIT-TO ( PUBSTAGE , \"aip\" ) ) "
            self.re_search(article_query)
            self.download(article, self.isPartial)

        elif final <= 2000:  # 2000개 넘는것만 다운로드 진행 // 2000개 이상인거는 PASS.안해..!!
            final_query = re_query + " AND  ( LIMIT-TO ( PUBSTAGE , \"final\" ) ) "
            self.re_search(final_query)
            self.download(final, self.isPartial)

        elif article <= 2000:
            article_query = re_query + " AND  ( LIMIT-TO ( PUBSTAGE , \"aip\" ) ) "
            self.re_search(article_query)
            self.download(article, self.isPartial)

        else:
            print("다운로드 진행 불가")

    def over2000_years(self, start):
        count = 0
        for i in self.over_2000_papers:
            temp = self.query
            query_year = temp + ' ' + ' AND ( LIMIT-TO ( PUBYEAR, ' + i[0] + ') ) '
            if len(self.over_2000_papers) != 0:
                count = self.re_search(query_year)  # 2000개 이상의 연도 재검색
            self.crawl(query_year, count)
            self.over_2000_papers_country = []
        print(self.p, "종료")
        return 0

    def crawl(self, f_Query, count):
        try:
            n_query, All_open_Access, other = self.Access_type(f_Query, count)  # Access_type 파싱완료 (if All_open_Access<=2000: return exclude All_open_Access)
            acceessType_1 = 'AND ( LIMIT-TO ( freetoread ,"all"))'
            acceessType_2 = 'AND ( EXCLUDE ( freetoread ,"all"))'
            query_limit = ' AND ( LIMIT-TO ( AFFILCOUNTRY, '
            if n_query == True:  # Access_type으로 완료
                return
                # 종료
            elif All_open_Access == 0:  # open Access는 다운로드 완료. --> otehr 자르기
                # n_query = f_Query + acceessType_2
                self.re_search(n_query)  # other 재검색
                self.search_country(n_query, other)  # 나라로 2000개씩 다운완료
                print(self.p, "other access Country 완료")
                self.country_flag = True
                for idx in range(len(self.over_2000_papers_country)):  # 2000개 넘는 나라 키워드로 나누기
                    if self.crawl_error == False:
                        query_country = n_query + query_limit + self.over_2000_papers_country[idx][0] + ') ) '  # "access_type + 나라" 쿼리 생성
                        count = self.re_search(query_country)  # 재검색
                        self.search_keyword(query_country, count)  # 키워드 함수 이동(재검색 키워드, 재검색 논문수 )
                    else:
                        return
                print(self.p, "other access 완료")

            elif other == 0:  # other 다운로드 완료. --> open_Access 자르기  (거의 없을듯....)
                # n_query = f_Query + acceessType_1
                self.re_search(n_query)  # other 재검색
                self.search_country(n_query, All_open_Access)  # 나라로 2000개씩 다운완료
                print(self.p, "open access Country 완료")
                self.country_flag = True
                for idx in range(len(self.over_2000_papers_country)):  # 2000개 넘는 나라 키워드로 나누기
                    if self.crawl_error == False:
                        query_country = n_query + query_limit + self.over_2000_papers_country[idx][0] + ') ) '  # "access_type + 나라" 쿼리 생성
                        count = self.re_search(query_country)  # 재검색
                        self.search_keyword(query_country, count)  # 키워드 함수 이동(재검색 키워드, 재검색 논문수 )
                    else:
                        return
                print(self.p, "open access 완료")

            else:  # 둘다 2000개 넘을 때
                n_query = f_Query + acceessType_1
                self.re_search(n_query)  # open 재검색
                self.search_country(n_query, All_open_Access)  # 나라로 2000개씩 다운완료
                print(self.p, " open access type Country 완료")
                self.country_flag = True
                for idx in range(len(self.over_2000_papers_country)):  # 2000개 넘는 나라 키워드로 나누기
                    if self.crawl_error == False:
                        query_country = n_query + query_limit + self.over_2000_papers_country[idx][0] + ') ) '  # "access_type + 나라" 쿼리 생성
                        count = self.re_search(query_country)  # 재검색
                        self.search_keyword(query_country, count)  # 키워드 함수 이동(재검색 키워드, 재검색 논문수 )
                    else:
                        return
                print(self.p, " open access 종료")

                n_query = f_Query + acceessType_2
                self.re_search(n_query)  # other 재검색
                self.search_country(n_query, other)  # 나라로 2000개씩 다운완료
                print(self.p, " other type Country 완료")
                self.country_flag = True
                for idx in range(len(self.over_2000_papers_country)):  # 2000개 넘는 나라 키워드로 나누기
                    if self.crawl_error == False:
                        print(self.p, len(self.over_2000_papers_country) - (idx + 1), '개 country 남음')
                        query_country = n_query + query_limit + self.over_2000_papers_country[idx][0] + ') ) '  # "access_type + 나라" 쿼리 생성
                        count = self.re_search(query_country)  # 재검색
                        self.search_keyword(query_country, count)  # 키워드 함수 이동(재검색 키워드, 재검색 논문수 )
                    else:
                        return
                print(self.p, " other access 종료")
            # the end 끄읏
            return
        except:
            return
# ===================================================================================
#===============================================================================================================
