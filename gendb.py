import time
import requests
import sqlalchemy

from math import trunc
from random import randint, random
from random import seed
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine,Column,String,Integer,Float
from flask import Flask

app = Flask(__name__)

def getList(url):
    response = requests.get(url).json()
        
    # extract results
    results = response['results']

    cond = True
    while cond:

        for entry in results:
            namesChar.append(entry['name'])
        if response['next'] == None: break
        response = requests.get(response['next']).json()
        # if response != 200: cond = False
        # if response.status_code != 200 : cond=False
        # extract results
        results = response['results']


@app.route('/refresh')
def create_salesdb():
    # Global variables
    namesChar = []
    print('Creating database')
    base_url = 'https://swapi.dev/api/'

    # Create Database Connection
    # ----------------------------------
    database_path = "salesdb"
    engine = create_engine(f"sqlite:///{database_path}")
    # conn = engine.connect()
    base = declarative_base()

    # Use this to clear out the db
    # ----------------------------------
    base.metadata.drop_all(engine)

    class Salesdb(base):
        __tablename__ = "salesdb"
        poster_content = Column(String(255),primary_key=True)
        quantity  = Column(Integer)
        price = Column(Float)
        email  = Column(String(50))
        sales_rep = Column(String(50))
        promo_code  = Column(String(10))

    # Create a "Metadata" Layer That Abstracts our SQL Database
    # ----------------------------------
    base.metadata.create_all(engine)


    # get the list of starships
    swURL = base_url + 'starships/'
    # print(swURL)

    getList(swURL)

    # get the list of planets
    swURL = base_url + 'planets/'
    # print(swURL)

    getList(swURL)

    # where does the character names start
    startName = len(namesChar)

    # get the list of characters
    swURL = base_url + 'people/'

    getList(swURL)

    # get the length of the array
    maxData = len(namesChar)

    # define the table and insert fake data

    session = Session(bind=engine)

    seed(1)
    email = 'test@gmail.com'
    sales_rep = ''
    for i in range(10):
        count = randint(1,10)
        price = round(random() * 10,3)
        # ran = len(namesChar)/10
        name = namesChar[randint(0,maxData)]
        sales = namesChar[randint(0,maxData-startName) + startName]
        sales_split = sales.rsplit(' ')
        sales_rep=sales_split[0] + '@swmail.com'
        print(name,count,price,sales_rep)
        promoCode = ' '
        if randint(0,10) == 1: promoCode = 'mobile'
        try:
            new_rec = Salesdb(poster_content=name,quantity=count,price=price,email=email, sales_rep=sales_rep,promo_code=promoCode)
            session.add(new_rec)
            session.commit()
        except:
            # Use this to clear out the db
            # ----------------------------------
            base.metadata.drop_all(engine)
            break

    

@app.route('/')
def hello():
    create_salesdb()
    return 'Hello Star Wars fans!'