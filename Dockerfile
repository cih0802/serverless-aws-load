# AWS에서 제공하는 Lambda용 공식 파이썬 3.12 이미지 사용
FROM public.ecr.aws/lambda/python:3.12

# 1. 요구사항 파일 복사 및 설치
COPY requirements.txt ${LAMBDA_TASK_ROOT}
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# 2. 작성해둔 Snowflake Load 람다 코드 복사
# 기존: COPY lambda_function.py ${LAMBDA_TASK_ROOT}
# 변경: 모든 파이썬 파일을 복사하도록 수정
COPY *.py ${LAMBDA_TASK_ROOT}

# 3. 람다 실행 시 핸들러 지정
# 기본 CMD는 기존 Load 로직으로 유지
CMD [ "lambda_function.lambda_handler" ]
