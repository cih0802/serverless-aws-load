import os
import psycopg2
from psycopg2.extras import execute_values
import snowflake.connector

def lambda_handler(event, context):
    # Snowflake 접속 및 데이터 추출
    sn_conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASS'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='SANDBOX',
        schema='SILVER'
    )
    sn_cur = sn_conn.cursor()
    
    extract_query = """
        SELECT trade_date, ticker, usd_close_price, usd_krw_rate, 
               krw_close_price, usd_daily_return_pct, krw_daily_return_pct
        FROM fct_daily_investment_metrics
        WHERE trade_date >= DATEADD(day, -14, CURRENT_DATE())
    """
    sn_cur.execute(extract_query)
    records = [row for row in sn_cur] # Fetchall 대체 (메모리 최적화)
    
    sn_cur.close()
    sn_conn.close()

    if not records:
        return {"status": "success", "message": "새로 적재할 데이터가 없습니다."}

    # NeonDB 접속 및 Direct UPSERT
    pg_conn = psycopg2.connect(os.getenv('DATABASE_URL'))
    pg_cur = pg_conn.cursor()

    try:
        upsert_query = """
            INSERT INTO daily_investment_metrics (
                trade_date, ticker, usd_close_price, usd_krw_rate, 
                krw_close_price, usd_daily_return_pct, krw_daily_return_pct
            ) VALUES %s
            ON CONFLICT (trade_date, ticker) DO UPDATE SET
                usd_close_price = EXCLUDED.usd_close_price,
                usd_krw_rate = EXCLUDED.usd_krw_rate,
                krw_close_price = EXCLUDED.krw_close_price,
                usd_daily_return_pct = EXCLUDED.usd_daily_return_pct,
                krw_daily_return_pct = EXCLUDED.krw_daily_return_pct;
        """
        execute_values(pg_cur, upsert_query, records)
        pg_conn.commit()

        return {"status": "success", "message": f"NeonDB 적재 완료 ({len(records)} rows)"}

    except Exception as e:
        pg_conn.rollback()
        raise e
    finally:
        pg_cur.close()
        pg_conn.close()