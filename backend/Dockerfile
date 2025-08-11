# 공식 Python 런타임을 부모 이미지로 사용합니다.
FROM python:3.11-slim

# 컨테이너의 작업 디렉토리를 설정합니다.
WORKDIR /app

# requirements.txt를 먼저 복사하여 Docker 레이어 캐싱을 활용합니다.
COPY requirements.txt .

# 의존성을 설치합니다.
RUN pip install --no-cache-dir -r requirements.txt

# 나머지 백엔드 애플리케이션 코드를 복사합니다.
COPY . .

# 앱이 실행될 포트를 노출합니다.
EXPOSE 8000

# 애플리케이션 실행 환경 변수
ENV LOG_FILE_PATH=./app.log
ENV LOG_LEVEL=INFO

# 로그 디렉터리 생성
RUN mkdir -p /DATA/data-viewer/log

# 앱을 애플리케이션 내부 진입점으로 실행 (내부에서 log_config를 주입)
CMD ["python", "main.py"]