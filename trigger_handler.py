import os
import snowflake.connector

def lambda_handler(event, context):
    # Snowflake 연결
    conn = snowflake.connector.connect(
        account=os.environ.get('SNOWFLAKE_ACCOUNT'),
        user=os.environ.get('SNOWFLAKE_USER'),
        password=os.environ.get('SNOWFLAKE_PASSWORD'),
        warehouse='COMPUTE_WH',
        database='SANDBOX',
        schema='SILVER',
        role='SYSADMIN'
    )
    
    try:
        # 루트 태스크 실행 (이후 하위 태스크인 지표 계산은 자동 연쇄 실행됨)
        sql_query = "EXECUTE TASK SANDBOX.SILVER.TSK_MERGE_EXCHANGE_RATE;"
        
        for cur in conn.execute_string(sql_query):
            # 🌟 문제의 코드 수정 (statusmessage -> fetchall)
            result = cur.fetchall()
            print(f"Task triggered. Result: {result}")
        
        return {
            'status': 'success',
            'message': 'Snowflake Transform Tasks Triggered Successfully'
        }
        
    except Exception as e:
        print(f"Error triggering task: {e}")
        raise e
        
    finally:
        conn.close()
        