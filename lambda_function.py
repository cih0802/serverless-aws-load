import os
import json
from datetime import datetime
import snowflake.connector

def lambda_handler(event, context):
    # 1. 입력값(Event) 분석
    # Step Functions에서 { "data_type": "KEXIM", "target_date": "2026-03-12" } 식으로 전달 예정
    data_type = event.get('data_type', 'KEXIM') 
    
    # 🌟 Step Functions에서 넘겨준 원본 입력값 추출
    original_event = event.get('original_input', event)

    # original_event에서 날짜를 찾습니다.
    if 'target_date' in original_event:
        execution_date = datetime.strptime(original_event['target_date'], '%Y-%m-%d')
    elif 'time' in original_event:
        execution_date = datetime.strptime(original_event['time'], '%Y-%m-%dT%H:%M:%SZ')
    else:
        execution_date = datetime.utcnow()

    year, month, day = execution_date.strftime('%Y'), execution_date.strftime('%m'), execution_date.strftime('%d')
    search_date_str = execution_date.strftime('%Y%m%d')

    # 2. 데이터 타입별 쿼리 분기
    if data_type == 'KEXIM':
        table_name = "raw_exchange_rate"
        s3_path = f"exchange_rate/year={year}/month={month}/day={day}/"
        # KEXIM은 JSON 배열로 들어오므로 $1 그대로 저장
        select_stmt = f"$1, '{search_date_str}'"
    else:  # ETF 케이스
        table_name = "raw_etf"
        s3_path = f"etf_data/year={year}/month={month}/day={day}/"
        select_stmt = f"$1, '{search_date_str}'"

    # 3. Snowflake 연결
    conn = snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        warehouse='COMPUTE_WH',
        database='SANDBOX',
        schema='BRONZE',
        role='SYSADMIN'
    )
    
    try:
        sql_query = f"""
            COPY INTO {table_name} (raw_data, search_date)
            FROM (
              SELECT {select_stmt}
              FROM @my_s3_stage/{s3_path}
            )
            FILE_FORMAT = (TYPE = JSON)
            ON_ERROR = 'CONTINUE';
        """
        
        for cur in conn.execute_string(sql_query):
            result = cur.fetchall()
            print(f"Query executed for {data_type}. Result: {result}")
            
        return {
            'status': 'success',
            'data_type': data_type,
            'loaded_date': search_date_str
        }
        
    except Exception as e:
        print(f"Error loading {data_type}: {e}")
        raise e
    finally:
        conn.close()