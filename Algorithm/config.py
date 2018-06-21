
import psycopg2

conn = psycopg2.connect("dbname='$DATABASE_NAME' user='$USER_NAME' host='localhost' password='$PASSWORD'")
cur = conn.cursor()

api_key = '$API_KEY'
api_secret = '$API_SECRET'

recv_window = 5000

# Enter ticker symbol

ticker = ['btcusdt']
