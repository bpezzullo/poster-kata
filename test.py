from math import trunc
from random import randint, random
from random import seed

import requests
import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session,relationship
from sqlalchemy import create_engine,Column,String,Integer,Float,Date,Table
from sqlalchemy.sql.schema import ForeignKey
# import psycopg

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
    return

def getDetails(url):
    response = requests.get(url).json()
        
    # extract results
    results = response['results']

    cond = True
    count = 0
    filmCnt = 0
    while cond:
        for entry in results:
            count += 1
            print('Starship count:',count)
            starshipAr[entry['name']] = {'films': entry['films'],
                                            'date': entry['created'],
                                            'url': entry['url'],
                                            'id': count}
            try:
                for film in entry['films']:
                    if session2.query(Films).filter_by(name=film):
                        print('match film found')
                        # new_rec = starship_films(film_id = instan) 
                    else:
                        filmCnt += 1
                        new_rec = Films(id=filmCnt,
                                        name = film)
                        session2.add(new_rec)
                        #new_rec = starship_films(film_id = )
                        session2.commit()
            except:
                print("something went wrong")
                break
            try:
                new_rec = Starship(id = count,
                                    name = entry['name'],
                                    url = entry['url'],
                                    created = entry['created']
                                    )
                session2.add(new_rec)
                session2.commit()
            except:
                # Use this to clear out the db
                # ----------------------------------
                base2.metadata.drop_all(engine2)
                print('could not write Starship')
                break

        if response['next'] == None: break
        response = requests.get(response['next']).json()
        # if response != 200: cond = False
        # if response.status_code != 200 : cond=False
        # extract results
        results = response['results']
    return

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
# base.metadata.drop_all(engine)
print('dropped')

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

# check whether there is a salesdb table
session = Session(bind=engine)
results = session.query(Salesdb).first()
if not results:

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


for instance in session.query(Salesdb).all():  
    print("Poster: ", instance.poster_content)
    print("count: ", instance.quantity)
    print("---------")




# Pull the data from the salesdb and the SW API.

database_path='dw'
engine2 = create_engine(f"sqlite:///{database_path}")
# conn = engine.connect()
base2 = declarative_base()

# Use this to clear out the db
# ----------------------------------
#base2.metadata.drop_all(engine2)

starships_films_association = Table('starship_films', base2.metadata,
    Column('film_id', Integer, ForeignKey('films.id')),
    Column('starship_id', Integer, ForeignKey('starship.id'))
    )


class Sales(base2):
    __tablename__ = "sales"
    id = Column(Integer,primary_key=True)
    starship_id = Column(Integer, ForeignKey('starship.id'))
    total = Column(Float)
    quantity = Column(Integer)
    promo = Column(String(10))
    starship = relationship('Starship')

class Starship(base2):
    __tablename__ = "starship"
    id = Column(Integer,primary_key=True)
    name = Column(String(255))
    url = Column(String(255))
    created = Column(String(20))

class Films(base2):
    __tablename__ = "films"
    id = Column(Integer,primary_key=True)
    name = Column(String(255))

# Create a "Metadata" Layer That Abstracts our SQL Database
# ----------------------------------
base2.metadata.create_all(engine2)

session2 = Session(bind=engine2)

results = session2.query(Starship).first()
print(results)
if results:
    for instance in session2.query(Starship).all():  
        print("Poster: ", instance.id)
        print("count: ", instance.name)
        print("Total: ",instance.created)
        print("---------")
    
# clear out list
starshipAr = {}

# get the list of starships
swURL = base_url + 'starships/'
# print(swURL)

getDetails(swURL)

count = 0
print(starshipAr)
for instance in session.query(Salesdb).all(): 
    # Check if it is a starship 
    print(" final loop", instance.poster_content)
    if instance.poster_content in starshipAr.keys(): 
    	
        # we have a starship poster, save it into the DW
        # call the API to return additional information. One thought would be to pull all
        # data at one time and than parse the 
        count += 1
        print(count)
        try:
            new_rec = Sales(id = count,
                                starship_id = starshipAr[instance.poster_content].count,
                                total = instance.quantity * instance.price,
                                quantity = instance.quantity,
                                price = instance.price,
                                promo = instance.promo_code
                                )
            session2.add(new_rec)
            session2.commit()
        except:
            # Use this to clear out the db
            # ----------------------------------
            base2.metadata.drop_all(engine2)
            break

print('Final Display')
for instance in session2.query(Sales).all():  
    print("Poster: ", instance.starship_id)
    print("count: ", instance.quantity)
    print("Total: ",instance.total)
    print("---------")