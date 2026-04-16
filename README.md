# BLD Pharm MSDS 정보 추출기

CAS 번호 목록이 담긴 Excel 파일을 업로드하면 [BLD Pharm](https://www.bldpharm.com/) SDS PDF를 자동으로 다운로드하고 아래 정보를 추출합니다.

| 열 | 내용 |
|---|---|
| B | Product Name |
| C | Catalog Number |
| D | CAS Number |
| E | Appearance |
| F | 보관 조건 (상온 / 냉장 / 냉동) |
| G | 용량별 가격 (예: 1g/$11, 5g/$31) |

## 사용 방법

1. A열에 CAS 번호가 입력된 Excel 파일 업로드 (예: `64-17-5`)
2. **MSDS 정보 추출 시작** 버튼 클릭
3. 결과 확인 후 **결과 Excel 다운로드**

## 배포 (Streamlit Community Cloud)

1. 이 저장소를 fork 또는 clone
2. [share.streamlit.io](https://share.streamlit.io) 접속 → GitHub 계정 연동
3. `New app` → 이 저장소 선택 → Main file: `app.py` → Deploy

## 로컬 실행

```bash
pip install -r requirements.txt
streamlit run app.py
```
