import os
import sqlalchemy.engine as eng
from sqlalchemy.sql import text
import pandas as pd
from dotenv import load_dotenv

# load the .env file variables
load_dotenv()

files = ['src/sql/create.sql', 'src/sql/insert.sql', 'src/sql/drop.sql']

def connect():
    global engine
    st = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
    engine = eng.create_engine(st)
    pop_goes_the_sql()
    lock_goes_the_sql()
    drop_goes_the_sql()

def pop_goes_the_sql():
    global engine
    global files
    for i in range(2):
        with engine.connect() as con:
            with open(files[i]) as file:
                con.execute(text(file.read()))
                con.close()                

def lock_goes_the_sql():
    global engine
    df = pd.read_sql('books', engine.connect())
    print(df)

def drop_goes_the_sql():
    global engine
    global files
    with engine.connect() as con:
        with open(files[-1]) as file:
            query = text(file.read())
            con.execute(query)

connect()