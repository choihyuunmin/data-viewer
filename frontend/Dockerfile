# 스테이지 1: Vue 애플리케이션 빌드
FROM node:20-alpine as builder

# 작업 디렉토리 설정
WORKDIR /app

# 빌드 인자 선언
ARG VITE_DATAVIEWER_ENV=prod
ENV VITE_DATAVIEWER_ENV=$VITE_DATAVIEWER_ENV

# package.json과 package-lock.json을 복사합니다.
COPY package*.json ./

# npm ci를 사용하여 의존성을 설치합니다. (더 빠르고 안정적입니다)
RUN npm install

# 나머지 소스 코드를 복사합니다.
COPY . .

# 프로덕션용으로 애플리케이션을 빌드합니다.
RUN npm run build

# 스테이지 2: Nginx로 애플리케이션 서비스
FROM nginx:stable-alpine

# envsubst를 사용하기 위해 gettext 패키지 설치
RUN apk add --no-cache gettext

# 빌드 스테이지에서 빌드된 정적 파일들을 Nginx의 기본 html 디렉토리로 복사합니다.
COPY --from=builder /app/dist /usr/share/nginx/html

# SPA 라우팅을 위한 Nginx 설정 파일을 템플릿으로 복사합니다.
COPY nginx.conf /etc/nginx/templates/default.conf.template

# 80번 포트를 노출합니다.
EXPOSE 80

# 환경변수를 nginx 설정에 치환하고 시작
CMD ["/bin/sh", "-c", "envsubst '${BACKEND_HOST}' < /etc/nginx/templates/default.conf.template > /etc/nginx/conf.d/default.conf && nginx -g 'daemon off;'"]