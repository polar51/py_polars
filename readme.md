# polars 테스트

# 🚀 데이터 분석 스크립트 실행 가이드

이 가이드는 파이썬이 설치되지 않은 윈도우(Windows) 환경에서 프로젝트를 세팅하고 실행하는 방법을 설명합니다.

## 1. 사전 준비 (파이썬 설치)
회사 컴퓨터에 파이썬이 없다면 먼저 설치해야 합니다.

1. [Python 공식 홈페이지 다운로드](https://www.python.org/downloads/) 접속
2. 최신 버전(예: 3.12.x 등) 다운로드 및 실행
3. **[중요]** 설치 화면 하단의 **Checking Box: `Add Python to PATH`** 를 반드시 체크하세요!
4. `Install Now` 클릭하여 설치 완료

---

## 2. 프로젝트 폴더 세팅
바탕화면 등 원하는 위치에 폴더를 만들고 아래 파일들을 준비합니다.

* `main.py` (작성한 코드)
* `siv_Inverter.csv` (분석할 데이터 파일)

---

## 3. 가상환경(.venv) 생성 및 라이브러리 설치
프로젝트 폴더 내에서 `PowerShell`을 열고 아래 명령어들을 순서대로 입력합니다.

### 3-1. 가상환경 생성
프로젝트만의 독립된 공간(.venv)을 만듭니다.
```powershell
python -m venv .venv

```aiignore
프롬프트
여기서 수정만 하자 일단 root 폴더 내에 siv_Inverter.csv라는 파일의 데이터를 가져올거라 테스트용 더미 CSV는 필요 없어 그리고 알고리즘이 이렇게 필요해



1. CSV 파일 데이터 중에 첫 로우부터 분석을 할거야 car1_value, car8_value 필드의 두 값이 각각 0.02가 넘는지 확인하고 넘지 않는 로우는 버려 그럼 남은 로우는 두 값 모두 0.02를 넘는거야

2. 0.02가 넘는 로우들을 car1_value, car8_value를 기준으로 나누어 즉 한개의 로우에서 oper_datetime, fleet_id, value, car_no 로 car1_value면 carNo가1 car8이면 carNo가 8로 나누어지는거야

3. A 알고리즘

3-1. value가 1,000을 넘는 로우들을 따로 분류해줘

3-1-1. 1000을 넘긴 로우들의 value값이 1160의 플러스 마이너스 오차범위 5% 안에 해당한다면[{oper_datetime: oper_datetime, fleet_id: fleet_id, car_no: car_no, event_no:  "과전류 검지"},...] 의 형태로 만들어서 print로 보여주거나 txt 파일로 만들어줘

3-1-2. 1000을 넘지 못한 로우들의 값이 547 플러스 마이너스 오차범위 5%를 oper_datetime를 기준으로 1시간 동안 연속적으로 유지한다면 [{oper_datetime: oper_datetime, fleet_id: fleet_id, car_no: car_no, event_no:  "과부하 검지"},...]

의 형태로 만들어서 print로 보여주거나 txt 파일로 만들어줘



4. B 알고리즘

4-1. fleet_id > car_no를 그룹핑하여 동일한 fleet_id, car_no를 가진 value들의 나를 제외한 전체 평균을 내고 표준 편차>25% 인 값들을 "이상 전류 검지" 라는 스트링 값으로 해서 [{oper_datetime: oper_datetime, fleet_id: fleet_id, car_no: car_no, event_no:  "이상 전류 검지"},...] 의 형태로 만들어서 print로 보여주거나 txt 파일로 만들어줘
```

실행어 `python main.py`  

---
현재 정상적으로 작동하나 추후 데이터를 받아오는 방식, 보내는 방식 등을 변경이 필요함
## 안정화 포인트
1. 현재는 CSV 파일이지만 Parquet로 변환하는 과정을 추가하면 5TB의 데이터에도 뻗지 않음
2. 알고리즘 부분은 추후에 공통 로직으로 만들어서 매개변수를 통해 모든 부분에서 사용 할 수 있게 변경 가능( 유지 보수성 )

3. 
