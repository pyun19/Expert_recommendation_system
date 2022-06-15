# 데이터 기반 전문가 추천 시스템

## 📖 상세 내용
대학원 연구실에서 진행한 프로젝트입니다. 전문가를 판별할 수 있는 중요한 지표인 논문, 특허, 보고서 등의 데이터를 활용하여 사용자가 입력한 전문가 추천 지수를 통해 사용자의 요구에 적합한 전문가를 추천하는 시스템입니다. 검색한 분야의 전문가 순위 뿐만 아니라, 각 전문가의 관련 데이터와 관계 정보까지 제공합니다.

사용자가 원하는 검색 키워드를 입력
![image](https://user-images.githubusercontent.com/62095363/173772811-d3c2dcd9-628f-4f7e-8947-0e08ca79e307.png)

사용자가 원하는 전문가 지수 입력
![image](https://user-images.githubusercontent.com/62095363/173772859-6f7cd4dc-073f-4816-b7b4-94dbbc2cf0d1.png)

결과 출력(전문가 순위 및 전문가 관계 정보)
![image](https://user-images.githubusercontent.com/62095363/173772898-757e9839-2c02-4b38-8030-55b629e9db61.png)
![image](https://user-images.githubusercontent.com/62095363/173772923-8d857038-e6dd-4b57-be58-ab5a13470534.png)


## 🛠️ 사용 기술 및 라이브러리

- Python3
- MongoDB
- Kafka
- Data Crawling
- Linux


## 📱 담당한 기능

![image](https://user-images.githubusercontent.com/62095363/173773184-90148c12-a805-4704-a537-4c90f801b6cd.png)

- 국내 및 해외 학술 검색 사이트 **Open API/Crawling**을 통한 **데이터 수집** 및 **데이터 전처리기 개발**
- **Message Queue(Apache Kafka)**를 이용한 **실시간 데이터 처리 시스템 개발**
- **MongoDB** 데이터 **저장 및 관리**
- **Multi Processing**을 적용하여 기존 수집기 대비 **처리 속도 향상(250% 이상 향상)**
- **MongoDB** 복합 인덱스를 통한 slow query 튜닝 개선
- 동일 저자 판별 알고리즘 개발(약칭, 풀네임, Ms, Mr, 소속 변경 등 비교 및 판별)


## 💡 깨달은 점

- 매주 팀 및 팀원 간 소통의 중요함
- Python3의 Watch dog 라이브러리로 이벤트성 파일 크롤링을 할 수 있음
- 한번에 너무 많은 기계적 크롤링 접속은 사이트의 차단 또는 오류를 유발함.(403, 503 에러)
- MongoDB는 NoSQL manager로 Linux 환경보다 쉽게 다룰 수 있음
