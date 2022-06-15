import requests, xmltodict, datetime, openpyxl, asyncio, functools
import sys, os, re, math, logging, time, json, xlrd, retry, pprint
from selenium.common.exceptions import ElementNotVisibleException
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.alert import Alert
from multiprocessing import Process, Value, Array
from multiprocessing.managers import BaseManager
from selenium.webdriver.common.keys import Keys
from pyvirtualdisplay import Display
from collections import OrderedDict
from urllib.request import urlopen
from ParsingHelper import Parser
from pymongo import MongoClient
from kafka import KafkaProducer
from selenium import webdriver
from bs4 import BeautifulSoup
from langdetect import detect
from threading import Thread
from random import randint
from json import dumps
from time import sleep
import logging as log
import csv
import random


class WOS_crawler:
    '''
    @ Method Name     : __init__
    @ Method explain  : WOS 크롤링 파라미터
    @ query_keyword   : 검색 연산처리(AND OR NOT) 된 키워드
    @ crawl_end       : 크롤링이 끝나면 = 1 // 끝나지 않으면 = 0 (프로세스 수 == crawl_end 수)
    @ parse_end       : 파싱이 끝나면 = 1 // 끝나지 않으면 = 0
    @ parse_data      : 파싱된 데이터 수
    @ num_data        : 웹 사이트로부터 다운로드 받은 수
    @ keyword         : 검색 기워드
    @ keyId           : 검색 키 아이디
    @ year            : 검색 년도
    @ total_data      : 총 데이터 수
    @ path            : 다운로드 저장 경로
    @ isPartial       : 부분적으로 크롤링 했다면(실패로 인한 스킵이 있었다면) (1 = True / 0 = False)
    @ is_not_active   : HTTP code확인(403 error) (1 = True / 0 = False) <-- 한번에 많은 데이터를 다운로드 받거나 기계적 접근이 감지 되면 차단당함
    @ korea_option    : 한국 논문만 볼 수 있는 옵션 (0 = True / 1 = False)
    @ chrome_failed   : 크롬 드라이버 정상 작동 확인 (1 = True / 0 = False)
    '''
    def __init__(self, query_keyword, crawl_end, parse_end, parse_data, num_data, keyword, keyId, year, total_data, path, isPartial, is_not_active, korea_option, chrome_failed, url, etc_path):
        self.crawl_end = crawl_end
        self.parse_end = parse_end
        self.num_data = num_data
        self.num_parse = parse_data
        self.keyword = keyword
        self.keyId = keyId
        self.year = year
        self.query_keyword = query_keyword
        self.path = path
        self.is_not_active = is_not_active
        self.korea_option = korea_option
        self.korea_search = ''
        self.chrome_failed = chrome_failed
        self.options = webdriver.ChromeOptions()
        self.options_box = ['--disable-dev-shm-usage',"lang=ko_KR",'--no-sandbox',
        'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36']
        for i in range(0,4):
            self.options.add_argument(self.options_box[i])
        self.options.add_experimental_option("prefs", {
        "download.default_directory" : self.path,
        "download.prompt_for_download" : False,
        'profile.default_content_setting_values.automatic_downloads': 1,
        })
        self.driver = webdriver.Chrome(etc_path + "/chromedriver", options = self.options)
        self.frist_down = 0
        self.nowset_cnt = 0
        self.frist_checkbox = 0
        self.down_count = 0
        self.s_num = 1
        self.paper_cnt = 0
        self.exception_cnt = 0
        self.nowset_cnt = 0
        self.isPartial = isPartial
        self.frist_checkbox = 0
        self.frist_down = 0
        self.select_count = 0
        self.total_data = total_data
        self.total_paper_count = 0
        self.error_count = []
        self.end_num = 0
        self.start_num = 0
        self.total_record = 0
        self.total_select_count = 0
        self.temp = {}
        self.max_set_cnt = 0
        self.last_check = 0
        self.url = url
# '''#######################################################################################################################################'''
    '''
    @ Method Name     : check_file
    @ Method explain  : 파일 생성 확인 --> 마지막 폴더&파일 전체 삭제
    '''
    def check_file(self):
        check_file_list = os.listdir(self.path)
        if check_file_list:
            for i in range(len(check_file_list)):
                check_file = check_file_list[i]
                check_file_path = (self.path + '/' + check_file)
                os.remove(check_file_path)
            print("file 리스트 전체 삭제(초기화)")

        else:
            print("file 리스트 PASS(NULL)")
            pass
        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : WOS_crawl_end
    @ Method explain  : 크롤링 종료 --> 파일 생성
    '''
    def WOS_crawl_end(self):
        print('========================  Crawling을 종료 합니다.  ========================')
        if self.last_check == 0:
            self.last_check = 1
            time.sleep(2)
            self.num_data.value = self.num_parse.value
            with open(self.path + "/crawler_end.txt", 'w') as f:
                f.write("--end--")
            print('마지막 데이터 다운로드 실패 ===> 종료 파일을 생성합니다. ')
        return
#
# '''#######################################################################################################################################'''
    '''
    @ Method Name     : WOS_checkbox
    @ Method explain  : 파일 다운로드 목록 선택
    '''
    def WOS_checkbox(self):
        self.driver.find_element_by_css_selector('#CONFERENCE_INFO_fields').click()  # 학회정보 해제
        time.sleep(1)
        self.driver.find_element_by_css_selector('#CITTIMES_fields').click()  # 인용횟수 해제
        time.sleep(1)
        self.driver.find_element_by_css_selector('#ISSN_fields').click()  # ISSN해제
        time.sleep(1)
        self.driver.find_element_by_css_selector('#PMID_fields').click()  # PMID 해제
        time.sleep(1)
        self.driver.find_element_by_css_selector('#ABSTRACT_fields').click()  # 초록 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#DOCTYPE_fields').click()  # 문서유형 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#SABBR_fields').click() # 원본 약어 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#ADDRS_fields').click()  # 연구기관명및주소 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#KEYWORDS_fields').click()  # 키워드 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#PUBINFO_fields').click()  # 출판사정보 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#CITREFC_fields').click()  # 인용문헌수 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#PAGEC_fields').click()  # 페이지수 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#LANG_fields').click()  # 언어 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#JCR_CATEGORY_fields').click()  # WOS범주 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#SUBJECT_CATEGORY_fields').click()  # 연구분야 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#USAGEIND_fields').click()  # 이용횟수 체크
        time.sleep(1)
        self.driver.find_element_by_css_selector('#record_select_type_range').click()  # 레코드x-y범위 클릭
        time.sleep(1)
        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : check_403_error
    @ Method explain  : 403 에러 확인 (파일을 많이 OR 오래 받으면 사이트에서 차단)
    '''
    def check_403_error(self): #403 에러 확인 (파일을 많이 OR 오래 받으면 사이트에서 차단)
        url = self.url

        try:
            resq = requests.get(url, timeout = 30)
            rescode = resq.status_code

        except Exception as e:
            print(e)

        print("사이트 Request 코드 확인 : ", rescode) # 403 에러 확인


        if not rescode == 200:
            print('=================', rescode,'사이트 에러 입니다.','=================')
            self.is_not_active.value = 1

        time.sleep(3)
        return


# '''#######################################################################################################################################'''
    '''
    @ Method Name     : like_human
    @ Method explain  : 사람인척하기 (랜덤하게 스크롤 하기)
    '''
    def like_human(self):
        print("### 사람인척하기 ###")
        for i in range(1, random.randrange(10,30)):
            self.driver.execute_script('window.scrollTo(0, %d00);' %i)
            time.sleep(0.1)
        print("================= 사람인척 종료 =================")

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : WOS
    @ Method explain  : 사이트 접속후 크롤링
    '''
    def WOS(self):
        now = time.localtime()
        time_a = time.gmtime(time.time())
        url = self.url

        if self.korea_option == 0:
            self.korea_search = ' AND CU = SOUTH KOREA'

        try:
            try:
                self.driver.get(url) # WOS 사이트접속
                self.driver.implicitly_wait(10)
                self.driver.delete_all_cookies()
                print("사이트 접속 완료 ===> 쿠키 삭제")
                time.sleep(3)
                print('고급 검색 창 이동')

            except:
                self.chrome_failed.value = 1
                raise Exception('==============================Chrome 접속 실패==============================')

            try:
                self.driver.find_element_by_xpath('/html/body/app-wos/div/div/header/app-header/div[1]/button[3]').click()
                print("NEW_PAGE에서 Products click")
                time.sleep(2)
                self.driver.find_element_by_xpath('//*[@id="mat-menu-panel-5"]/div/div/a[1]').click()
                print("Classic WOS click")

                time.sleep(3)
                last_tab = self.driver.window_handles[-1]
                self.driver.switch_to.window(window_name=last_tab)
                print("Classic WOS 페이지 Tab 이동")


            except:
                pass


            try:
                try:
                    self.driver.find_element_by_xpath('/html/body/div[9]/div/ul/li[4]/a').click()
                    print("고급검색1 click")
                except:
                    self.driver.find_element_by_xpath('/html/body/div[1]/div[26]/div/ul/li[4]/a').click()
                    print("고급검색2 click")

            except:
                try:
                    self.driver.find_element_by_css_selector('body > div.EPAMdiv.main-container > div.block-search.block-search-header > div > ul > li.searchtype-sub-nav__list-item.searchtype-sub-nav__list-item--active > a')
                    print("고급검색3 click")
                except:
                    raise Exception('시작 페이지(고급 검색 이동) 실패 ====> 403 Error 체크하고 종료합니다.')

            time.sleep(5)
            self.driver.find_element_by_xpath('//*[@id="value(input3)"]/option[2]').click()
            time.sleep(2)
            print("문서유형 Article 선택")

            try:
                year_difference = now.tm_year - int(self.year)
                year_div = list(divmod(year_difference, 5))
                if year_difference < 5:
                    time.sleep(2)
                    self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY =' + str(self.year) + '-' + str(now.tm_year) + self.korea_search)
                    print(self.year,' 부터 ', now.tm_year, ' 까지 검색')
                    time.sleep(2)
                    self.driver.find_element_by_id('search-button').click()
                    print("검색 click")
                    time.sleep(4)
                    self.nowset_cnt += 1

                elif year_difference == 0:
                    time.sleep(2)
                    self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY =' + str(self.year) + self.korea_search)
                    print(self.year,' 년도 논문 검색')
                    time.sleep(2)
                    self.driver.find_element_by_id('search-button').click()
                    print("검색 click")
                    time.sleep(4)
                    self.nowset_cnt += 1

                else:
                    i = 1
                    start_year = int(self.year) -5
                    plus_year = int(self.year)
                    for i in range(int(year_div[0])):    ### 5 나눠서 나머지 까지 +1 ``
                        start_year += 5
                        plus_year += 5

                        time.sleep(2)
                        self.driver.execute_script('document.getElementById("value(input1)").value="";')
                        print("고급 검색 초기화")
                        time.sleep(2)
                        if plus_year >= now.tm_year:
                            plus_year = now.tm_year
                        self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY =' + str(start_year) + '-' + str(plus_year) + self.korea_search)
                        print(start_year,' 부터 ', plus_year, ' 까지 검색')
                        time.sleep(2)
                        self.driver.find_element_by_id('search-button').click()
                        print("검색 click")
                        start_year += 1
                        plus_year += 1
                        self.nowset_cnt += 1

                    if not year_difference % 5 == 0:
                        time.sleep(2)
                        self.driver.execute_script('document.getElementById("value(input1)").value="";')
                        print("고급 검색 초기화")
                        time.sleep(2)
                        if plus_year >= now.tm_year:
                            plus_year = now.tm_year
                            self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY =' + str(now.tm_year) + self.korea_search)
                            print(now.tm_year, '년도 검색')
                            time.sleep(2)
                            self.driver.find_element_by_id('search-button').click()
                            print("검색 click")
                            self.nowset_cnt += 1

                        else:
                            pass
                            self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY =' + str(plus_year) + '-' + str(now.tm_year) + self.korea_search)
                            print(plus_year,' 부터 ', now.tm_year, ' 까지 검색')
                            time.sleep(2)
                            self.driver.find_element_by_id('search-button').click()
                            print("검색 click")
                            self.nowset_cnt += 1

                    else:
                        pass

            except:
                raise Exception("########## 검색 결과가 없습니다. ##########")

            try:
                c = 0
                max_total_count_sum = 0
                for c in range(self.nowset_cnt):    # 총 PAPER 수 COUNT
                    c += 1
                    time.sleep(2)
                    max_total_count_info = self.driver.find_element_by_xpath('//*[@id="set_%s_div"]'%c)
                    max_total_count = int(max_total_count_info.text.replace(',',''))
                    max_total_count_sum += max_total_count

                self.total_data.value = int(max_total_count_sum)
                self.total_paper_count = int(max_total_count_sum)
                print('총 논문 수 : ', self.total_paper_count)

                if self.total_paper_count > 0:
                    pass

                else:
                    raise Exception("########## 검색 결과가 없습니다. ##########") # 한번 더 시도

            except:
                raise Exception("########## 검색 결과가 없습니다. ##########")

            if self.total_paper_count < 100000:

                print('총 논문수 : 10000개 이하입니다.')
                final_paper = ''

                if self.nowset_cnt > 1:
                    print('SET가 2개 이상입니다.')
                    for t in range(self.nowset_cnt):
                        t += 1
                        print('SET연산 조합')
                        if t == 1:
                            paper_sum_search_a = str('#') + str(t)
                            final_paper += paper_sum_search_a

                        else:
                            paper_sum_search_b = ' OR ' + str('#') + str(t)
                            final_paper += paper_sum_search_b

                    time.sleep(2)
                    self.driver.execute_script('document.getElementById("value(input1)").value="";')
                    print("고급 검색 초기화")
                    time.sleep(2)
                    self.driver.find_element_by_name('value(input1)').send_keys(final_paper)
                    print("SET 연산 실행", final_paper)
                    time.sleep(2)
                    self.driver.find_element_by_id('search-button').click()
                    print("검색 click")
                    self.nowset_cnt += 1
                    hit_count_info = self.driver.find_element_by_id('set_3_div')
                    hit_count = hit_count_info.text
                    self.total_paper_count = int(hit_count.replace(',',''))
                    print('SET 연산 논문 결과 : ', self.total_paper_count)

                else:
                    pass

                try:
                    self.driver.find_element_by_id('set_%d_div' %self.nowset_cnt).click()
                    print(self.nowset_cnt, '번째 SET 클릭(이동)')
                    time.sleep(10)

                    self.max_set_cnt = self.nowset_cnt

                    self.WOS_excel_download()

                    time_b = time.gmtime(time.time())
                    print('엑셀다운 시,분,초 : ', time_a.tm_hour, time_a.tm_min, time_a.tm_sec) #엑셀 다운 총 시간체크
                    print('엑셀다운 시,분,초 : ', time_b.tm_hour, time_b.tm_min, time_b.tm_sec)

                    if self.num_data.value == 0:
                        raise Exception("======================== Data download 실패  ========================")

                    print('========================  모든 Data download 완료  ========================')

                except:
                    raise Exception("Set 연산 검색 Exception 발생 ===> 종료전 403 check")

            elif self.total_paper_count > 100000:
                self.nowset_cnt = 0
                time.sleep(1)
                self.driver.find_element_by_xpath('//*[@id="selectallTop"]').click()             # 총 PAPER 검색 목록 삭제
                time.sleep(3)
                self.driver.find_element_by_xpath('//*[@id="deleteTop"]').click()
                time.sleep(3)
                set_cnt = 1 # 세트 수 세기

                for y in range(int(self.year), now.tm_year + 1):
                    time.sleep(2)
                    self.driver.execute_script('document.getElementById("value(input1)").value="";')
                    print("고급 검색 초기화")
                    time.sleep(3)
                    self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + 'AND PY = ', y, self.korea_search)
                    time.sleep(2)
                    print("키워드 입력")
                    self.driver.find_element_by_id('search-button').click()
                    print("검색 click")
                    time.sleep(4)
                    self.like_human()

                    hit_count_info = self.driver.find_element_by_xpath('//*[@id="hitCount"]')
                    hit_count = hit_count_info.text
                    hit_count = hit_count.replace(',','')
                    hit_count = int(hit_count)

                    print(y, "년도 검색 수 : ", hit_count)

                    if hit_count > 100000:
                        if self.korea_option == 1:
                            country_list = [' AND CU = USA', ' AND CU = PEOPLES R CHINA', ' NOT CU = USA NOT CU = PEOPLES R CHINA']
                            a = ['미국','중국', '그 외']
                            print(y,'년도 논문이 10만개 이상입니다. 나라별로 Crawling 합니다.')
                            time.sleep(3)
                            self.driver.find_element_by_css_selector('#deleteSets%d'%set_cnt).click()
                            time.sleep(3)
                            self.driver.find_element_by_css_selector('#deleteTop').click()
                            time.sleep(3)

                            for i in range(len(country_list)):
                                time.sleep(3)
                                self.driver.execute_script('document.getElementById("value(input1)").value="";')
                                print("고급 검색 초기화")
                                time.sleep(3)
                                self.driver.find_element_by_name('value(input1)').send_keys(self.query_keyword + ' AND PY = ', y, country_list[i])
                                time.sleep(3)
                                print("키워드 입력")
                                self.driver.find_element_by_id('search-button').click()
                                print("검색 click")
                                time.sleep(1)
                                self.like_human()

                                hit_count_info = self.driver.find_element_by_xpath('//*[@id="hitCount"]')
                                hit_count = hit_count_info.text
                                hit_count = hit_count.replace(',','')
                                hit_count = int(hit_count)

                                print(a[i], " 검색 논문 수 : ", hit_count)
                                time.sleep(4)
                                set_cnt += 1

                        else:
                            set_cnt += 1
                            pass

                    else:
                        set_cnt += 1
                        pass

                self.max_set_cnt = 1    # 최신 논문 부터 수집
                self.nowset_cnt = set_cnt
                for d in range(set_cnt, 1, -1):
                    self.down_count = 0
                    self.select_count = 0
                    self.nowset_cnt -= 1
                    self.s_num = 1

                    try:
                        self.driver.find_element_by_id('set_%d_div' %self.nowset_cnt).click()
                        print(self.nowset_cnt, '번째 SET 클릭(이동)', ' //  Max_set : ', set_cnt)
                        time.sleep(10)

                        self.WOS_excel_download()
                        time.sleep(5)

                        time_b = time.gmtime(time.time())
                        print('엑셀다운 시,분,초 : ', time_a.tm_hour, time_a.tm_min, time_a.tm_sec) #엑셀 다운 총 시간체크
                        print('엑셀다운 시,분,초 : ', time_b.tm_hour, time_b.tm_min, time_b.tm_sec)
                        print("예외 발생 수:  ", self.exception_cnt)

                    except:
                        self.isPartial.value = 1
                        print(self.nowset_cnt, '번째 SET 검색 실패')
                        pass

                    if self.nowset_cnt == self.max_set_cnt:
                        print('========================  모든 Data download 완료  ========================')


            else:
                print('Set 검색 갯수 Error')

        except Exception as e:
            print(e)

            if self.num_data.value == 0:
                print('========================   종료전 403 check   ========================')
                self.check_403_error()

            print("예외 발생 수:  ", self.exception_cnt)

        finally:
            print("예외 발생 수:  ", self.exception_cnt)
            self.WOS_crawl_end()


        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : total_record_count
    @ Method explain  : 총 논문수 계산 (최대 10000개 담을 수 있음)
    '''
    def total_record_count(self):
        total_count_info = WebDriverWait(self.driver, 240).until(lambda x : x.find_element_by_id('hitCount.top'))
        total_count = total_count_info.text
        total_count = total_count.replace(',', '')
        total_count = int(total_count)
        self.total_record = total_count

        if total_count > 100000:
            self.total_record = 100000
        print("총 논문 수 :    ", total_count)
        print("총 검색 최대 결과 수 :   ", self.total_record)
        self.total_select_count = self.total_record // 50000 + 1
        if (self.total_record % 50000 == 0):
            self.total_select_count -= 1
        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : select_list
    @ Method explain  : 선택 목록에 담기 (5000개씩 담을 수 있음)
    '''
    def select_list(self, e_num):
        self.driver.find_element_by_css_selector('#markedListButton > span > span').click()  # 선택목록에 추가 클릭
        time.sleep(2)
        print("선택목록에 담는 레코드 범위 추가 클릭")
        self.driver.find_element_by_css_selector('#numberOfRecordsRange').click()  # 레코드 범위 클릭
        time.sleep(2)
        self.driver.find_element_by_xpath('//*[@id="markFrom"]').clear()
        self.driver.find_element_by_xpath('//*[@id="markTo"]').clear()
        self.driver.find_element_by_xpath('//*[@id="markFrom"]').send_keys(self.s_num)
        print("장바구니 담기 시작 넘버 :  ",self.s_num)

        if self.total_record <= 50000:  # total_record가 오만보다 작으면 1~total_record까지의 범위 입력
            self.driver.find_element_by_xpath('//*[@id="markTo"]').send_keys(self.total_record)
            print("장바구니 담기 끝 넘버 :  ", self.total_record)
            self.driver.find_element_by_xpath('//*[@id="page"]/div[11]/div[2]/form/div[2]/span').click()  # 추가 클릭
            print("선택 목록 담기")
            print("1번째 선택 목록 담기 완료")
            time.sleep(10)
            self.driver.find_element_by_xpath('//*[@id="skip-to-navigation"]/ul[2]/li[4]/a').click()  # 선택목록으로 넘어가기
            print("선텍 목록 페이지 이동")
            time.sleep(6)
            if self.frist_checkbox == 1:
                self.WOS_checkbox()

        else:  # 오만보다 크면 카운트가 1 일때는 1~50000, 카운트가 2 일때는 50001~total_record까지 범위 입력(total_count는 최대 십만)
            if self.select_count == 1:
                self.driver.find_element_by_xpath('//*[@id="markTo"]').send_keys(e_num)
                print("장바구니 담기 끝 넘버 :  ", e_num)
                self.driver.find_element_by_xpath('//*[@id="page"]/div[11]/div[2]/form/div[2]/span').click()  # 추가 클릭
                time.sleep(10)
                print("선택 목록 담기")
                print("1번째 선택 목록 담기")
                self.driver.find_element_by_xpath('//*[@id="skip-to-navigation"]/ul[2]/li[4]/a').click()  # 선택목록으로 넘어가기
                time.sleep(6)
                print("선텍 목록 페이지 이동")

                if self.frist_checkbox == 1:
                    self.WOS_checkbox()

            else:
                self.driver.find_element_by_xpath('//*[@id="markTo"]').send_keys(self.total_record)
                print("장바구니 담기 끝 넘버 :  ", self.total_record)
                self.driver.find_element_by_xpath('//*[@id="page"]/div[11]/div[2]/form/div[2]/span').click()  # 추가 클릭
                time.sleep(10)
                print("선택 목록 담기")
                print("2번째 선택 목록 담기")
                self.driver.find_element_by_xpath('//*[@id="skip-to-navigation"]/ul[2]/li[4]/a').click()  # 선택목록으로 넘어가기
                time.sleep(6)
                print("선텍 목록 페이지 이동")
        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : total_record_info
    @ Method explain  : 레코드 수 계산 (500개씩 다운로드)
    '''
    def total_record_info(self):
        Select_record_info = self.driver.find_element_by_css_selector('#skip-to-navigation > ul.optionPanel.nav-list > li:nth-child(4) > a > span')
        Select_record = Select_record_info.text  # 선택 레코드 갯수
        Select_record = Select_record.replace(',', '')
        self.select_record = int(Select_record)
        self.down_num = self.select_record // 500 + 1
        if (self.select_record % 500 == 0):
            self.down_num -= 1
        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : record_download
    @ Method explain  : 선택한 파일 다운로드 하기
    '''
    def record_download(self):
        like_human_cnt = 0
        for Excel_down in range(0, self.down_num):
            self.frist_down += 1
            self.down_count += 1
            self.end_num = self.down_count * 500
            self.start_num = self.end_num - 499
            print("다운횟수 :   ", self.down_count)

            WebDriverWait(self.driver, 240).until(lambda x : x.find_element_by_xpath('//*[@id="markFrom"]')) # 페이지 완료까지 기다리기

            self.driver.find_element_by_xpath('//*[@id="markFrom"]').send_keys(self.start_num)
            time.sleep(random.randrange(0,5))
            print("시작 레코드범위 입력: ",self.start_num)

            if self.select_record <= self.end_num or self.down_count == self.down_num:
                self.driver.find_element_by_xpath('//*[@id="markTo"]').send_keys(self.select_record)
                time.sleep(random.randrange(0,5))
                print("끝 레코드범위 입력: ",self.select_record)

            else:
                self.driver.find_element_by_xpath('//*[@id="markTo"]').send_keys(self.end_num)
                time.sleep(random.randrange(0,5))
                print("끝 레코드범위 입력: ",self.end_num)

            if self.frist_down == 1:
                self.driver.find_element_by_xpath('//*[@id="exportTypeName"]').click()
                time.sleep(random.randrange(0,5))
                print("내보내기 클릭 next(Excel형식 클릭)")

                self.driver.find_element_by_xpath('//*[@id="saveToMenu"]/li[3]/a').click()
                time.sleep(3)
                print("Excel형식 클릭")

            else:
                self.download_click()

            try:
                self.num_data.value += 1
                try:
                    self.driver.find_element_by_xpath('//*[@id="page"]/div[24]/div[2]/form/div[2]/span/button').click()
                    print("내보내기(다운로드) 클릭")

                except:
                    self.driver.find_element_by_class_name('onload-primary-button').click()
                    print("내보내기(다운로드) 클릭")

                time.sleep(random.randrange(3,15))

                if (self.nowset_cnt == self.max_set_cnt) and (self.total_select_count == self.select_count):
                    if self.down_count == self.down_num:
                        self.num_data.value = self.num_parse.value
                        self.last_check += 1
                        with open(self.path + "/crawler_end.txt", 'w') as f:
                            f.write("--end--")
                        print('마지막 데이터 다운로드 성공 ===> 종료 파일을 생성합니다. ')


                if self.down_count == self.down_num:
                    time.sleep(3)

                    self.reset_click()
                    time.sleep(2)

                    self.reset.accept()

                    time.sleep(4)
                    print("선택목록을 초기화 합니다")

                    self.down_count = 0

                    self.driver.find_element_by_xpath('//*[@id="skip-to-navigation"]/ul[1]/li[2]/a ').click()
                    time.sleep(6)
                    print("검색 결과로 돌아갑니다")
                    break

                if like_human_cnt == 2:
                    self.like_human()
                    like_human_cnt = 0
                else:
                    like_human_cnt += 1

                WebDriverWait(self.driver, 60).until( lambda x : x.find_element_by_xpath('//*[@id="markFrom"]'))
                self.driver.find_element_by_xpath('//*[@id="markFrom"]').clear()
                self.driver.find_element_by_xpath('//*[@id="markTo"]').clear()

            except:
                self.num_data.value -= 1

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : download_click
    @ Method explain  : 다운로드 클릭 버튼 시도
    '''
    def download_click(self):
        try:
            try:
                self.driver.find_element_by_xpath('//*[@id="exportTypeName"]').click()
                print("내보내기 1클릭(exportTypeName)")

            except:
                self.driver.find_element_by_xpath('//*[@id="ml_output_options"]/table/tbody/tr[1]/td[5]/div[2]/span/nobr/div/div/div[1]/ul/li/button').click()
                print("내보내기 2클릭(xpath_1)")

        except:
            try:
                self.driver.find_element_by_xpath('//*[@id="ml_output_options"]/table/tbody/tr[1]/td[5]/div[2]/span/nobr/div/div/div[1]/button').click()
                print("내보내기 3클릭(xpath_1)")

            except:
                try:
                    self.driver.find_element_by_class_name('onload-secondary-button exportIconButton quickOutputExcel').click()
                    print("내보내기 4클릭(class_name)")

                except:
                    self.driver.find_element_by_id('exportTypeName').click()
                    print("내보내기 5클릭(id)")

        finally:
            time.sleep(random.randrange(2,7))
        # return
# '''#######################################################################################################################################'''
    '''
    @ Method Name     : reset_click
    @ Method explain  : 중간에 에러가 나면 다시 리셋하여 다시 시도
    '''
    def reset_click(self):
        try:
            try:
                WebDriverWait(self.driver, 10).until( lambda x : x.find_element_by_xpath('//*[@id="page"]/div[1]/div[26]/div[1]/div[2]/button[3]'))
                self.driver.find_element_by_xpath('//*[@id="page"]/div[1]/div[26]/div[1]/div[2]/button[3]').click() #선택목록을 초기화 시도
                print('초기화 버튼1 (xpath)')
            except:
                WebDriverWait(self.driver, 10).until( lambda x : x.find_element_by_css_selector('#page > div.EPAMdiv.main-container > div.markedListPage > div.NEWumlScoreCardContainer > div.ml-management > button.standard-button.secondary-button.snowplow-wos-markedlist-clear-click'))
                self.driver.find_element_by_css_selector('#page > div.EPAMdiv.main-container > div.markedListPage > div.NEWumlScoreCardContainer > div.ml-management > button.standard-button.secondary-button.snowplow-wos-markedlist-clear-click').click()
                print('초기화 버튼2 (css)')

        except:
            self.driver.find_elements_by_class_name('standard-button secondary-button snowplow-wos-markedlist-clear-click').click()
            print('초기화 버튼3 (class_name)')

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : comeback_start_page_reset
    @ Method explain  : 재시도 후 실패하면 다음 년도 파일 다운로드
    '''
    def comeback_start_page_reset(self):
        print("검색 리스트로 돌아갑니다")
        time.sleep(3)
        self.driver.find_element_by_xpath('/html/body/div[1]/h1/div/a/span').click() # wos 시작 페이졸 이동
        print("시작 페이지 이동")
        time.sleep(3)

        if self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button'):
            self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button').click()
        else:
            pass

        self.driver.find_element_by_xpath('//*[@id="skip-to-navigation"]/ul[2]/li[4]/a').click() #선택목록 클릭
        print("선택 목록 클릭")
        time.sleep(10)
        try:
            self.reset_click()

            time.sleep(3)
            self.reset.accept()
            print("초기화 엔터")
            time.sleep(3)
            self.driver.find_element_by_xpath('/html/body/div[1]/h1/div/a/span').click()
            print("시작 페이지 이동")

            if self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button'):
                self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button').click()
            else:
                pass

        except:
            self.driver.find_element_by_xpath('/html/body/div[1]/h1/div/a/span').click()
            print("시작 페이지 이동")
            time.sleep(3)

            if self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button'):
                self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button').click()
            else:
                pass
        try:
            try:
                self.driver.find_element_by_xpath('/html/body/div[9]/div/ul/li[4]/a').click()
                print("고급검색 click")
            except:
                self.driver.find_element_by_xpath('/html/body/div[1]/div[26]/div/ul/li[4]/a').click()
                print("고급검색 click")

        except:
            self.driver.find_element_by_css_selector('body > div.EPAMdiv.main-container > div.block-search.block-search-header > div > ul > li.searchtype-sub-nav__list-item.searchtype-sub-nav__list-item--active > a')
            print("고급검색 click")

        time.sleep(5)

        return

# '''#######################################################################################################################################'''
    '''
    @ Method Name     : WOS_excel_download
    @ Method explain  : 파일 다운로드 액션 실행
    '''
    def WOS_excel_download(self):
        self.reset = Alert(self.driver)

        try:
            self.total_record_count()

        except:
            print("예외 발생 ===> 뒤로 돌아갑니다.")
            self.isPartial.value = 1
            time.sleep(3)
            self.driver.execute_script('history.back();')
            time.sleep(3)
            self.exception_cnt += 1
            return

            # 선택목록에 추가
        self.frist_checkbox += 1
        for record_select in range(0, self.total_select_count):
            self.select_count += 1
            e_num = self.s_num + 49999


            try:
                self.select_list(e_num)

            except:
                time.sleep(5)
                self.driver.execute_script('location.reload()')
                print("레코드 입력 Exception 발생 새로고침 합니다.")
                time.sleep(60)

                self.select_list(e_num)

            # 엑셀 다운로드
            self.total_record_info()

            try:
                self.record_download()

            except:
                print("다운로드중 첫번째 Exception 발생")
                self.select_count -= 1
                self.down_count -= 1
                self.exception_cnt += 1
                # self.num_data.value -= 1
                time.sleep(2)

                try:
                    self.driver.execute_script('location.reload()')
                    print("새로고침 시도 합니다.")
                    self.comeback_start_page_reset()
                    time.sleep(5)

                    try:
                        self.driver.find_element_by_id('set_%d_div' %self.nowset_cnt).click()
                        print(self.nowset_cnt, '번째 SET 클릭(이동)')
                        time.sleep(10)
                    except:
                        print('1번째 Exception에서 Set 클릭 실패')

                    self.total_record_count()
                    # 선택목록에 추가

                    for record_select in range(0, self.total_select_count):
                        self.select_count += 1
                        try:
                            self.select_list(e_num)
                        except:
                            time.sleep(5)
                            self.driver.execute_script('location.reload()')
                            print("레코드 입력 Exception 발생 새로고침 합니다.")
                            time.sleep(15)
                            self.select_list(e_num)

                        # 엑셀 다운로드
                        self.total_record_info()

                        self.record_download()

                        self.s_num += 50000

                        if (self.total_select_count == self.select_count):
                            print("모든Excel 다운 완료")
                            print("검색 리스트로 돌아갑니다")

                            self.driver.find_element_by_xpath('/html/body/div[1]/h1/div/a/span').click()
                            print("시작 페이지 이동")

                            if self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button'):
                                self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button').click()
                            else:
                                pass

                            time.sleep(5)
                            try:
                                try:
                                    self.driver.find_element_by_xpath('/html/body/div[9]/div/ul/li[4]/a').click()
                                    print("고급검색1 click")
                                except:
                                    self.driver.find_element_by_xpath('/html/body/div[1]/div[26]/div/ul/li[4]/a').click()
                                    print("고급검색2 click")

                            except:
                                self.driver.find_element_by_css_selector('body > div.EPAMdiv.main-container > div.block-search.block-search-header > div > ul > li.searchtype-sub-nav__list-item.searchtype-sub-nav__list-item--active > a')
                                print("고급검색3 click")
                            return

                except:
                    self.isPartial.value = 1
                    print("다운로드 중 2번째 Exception 발생")

                    self.comeback_start_page_reset()
                    self.exception_cnt += 1
                    return

            self.s_num += 50000

            if (self.total_select_count == self.select_count):
                print("모든Excel 다운 완료")
                print("검색 리스트로 돌아갑니다")
                self.driver.find_element_by_xpath('/html/body/div[1]/h1/div/a/span').click()
                print("시작 페이지 이동")
                time.sleep(5)
                if self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button'):
                    self.driver.find_element_by_xpath('/html/body/div[17]/div/div/div[2]/button').click()
                else:
                    pass

                try:
                    try:
                        self.driver.find_element_by_xpath('/html/body/div[9]/div/ul/li[4]/a').click()
                        print("고급검색1 click")
                    except:
                        self.driver.find_element_by_xpath('/html/body/div[1]/div[26]/div/ul/li[4]/a').click()
                        print("고급검색2 click")

                except:
                    self.driver.find_element_by_css_selector('body > div.EPAMdiv.main-container > div.block-search.block-search-header > div > ul > li.searchtype-sub-nav__list-item.searchtype-sub-nav__list-item--active > a')
                    print("고급검색3 click")
                time.sleep(5)
                return
