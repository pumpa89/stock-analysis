import psycopg2
from sqlalchemy import create_engine
import pandas as pd
from batch_pipeline.config import DATE_FORMAT



class Database:
    def __init__(self):
        try:
            # Try PostgreSQL first
            self.engine = create_engine('postgresql://pumpaloski_h:@localhost/mse_stocks')
            self.conn = psycopg2.connect(
                dbname="mse_stocks",
                user="pumpaloski_h",
                password="",  # Add if you set one
                host="localhost"
            )
            self._ensure_table_exists()
            
        except Exception as e:
            print(f"⚠️ PostgreSQL failed: {e}. Using SQLite fallback...")
            # SQLite fallback
            import sqlite3
            self.engine = create_engine('sqlite:///mse_stocks.db')
            self.conn = sqlite3.connect('mse_stocks.db')
            self._ensure_table_exists()

    def _ensure_table_exists(self):
        """Create table if it doesn't exist"""
        with self.conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS stock_data (
                    issuer_code VARCHAR(10),
                    date DATE,
                    price NUMERIC(12, 2),
                    volume BIGINT,
                    PRIMARY KEY (issuer_code, date)
                )
            """)
            self.conn.commit()

    def get_last_date(self, issuer_code):
        """Get most recent date for an issuer"""
        query = """
            SELECT MAX(date) 
            FROM stock_data 
            WHERE issuer_code = %s
        """
        with self.conn.cursor() as cur:
            cur.execute(query, (issuer_code,))
            result = cur.fetchone()[0]
        return result.strftime(DATE_FORMAT) if result else None

    def save_data(self, dataframe):
        """Save DataFrame to database"""
        if not dataframe.empty:
            try:
                # Using SQLAlchemy for pandas compatibility
                dataframe.to_sql(
                    'stock_data',
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                print(f"✅ Saved {len(dataframe)} rows")
            except Exception as e:
                print(f"❌ Failed to save data: {e}")
                self.conn.rollback()

    def close(self):
        """Close connections"""
        self.conn.close()
        self.engine.dispose()