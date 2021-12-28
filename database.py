import psycopg2

class Database_Connector:
    @classmethod
    def connect(cls):
            conn =  psycopg2.connect(database='database' 
                            , user ='postgres'
                            , password='chakri123'
                            , host = '35.227.108.148'
                            , port = '5432')
            if conn : 
                print("Connection to the PostgreSQL established successfully.")
                return conn
            else :
                raise IOError("Connection to the PostgreSQL encountered and error.")

    @classmethod
    def get_ticker_data(cls,ticker,date):
        conn = Database_Connector().connect()
        curr = conn.cursor()

        try:
            curr.execute(f"SELECT * FROM dailydata WHERE date LIKE '{date}' AND symbol IN {ticker};")
            # curr.execute(f"SELECT * FROM dailydata WHERE symbol = 'AAPL' LIMIT 5;")
            data = curr.fetchall()
            curr.close()
            conn.close()
            print(f"Data retrival for {ticker} success")
            return data
        except:
            print(f"Data retrival failed for ticker {ticker}")

