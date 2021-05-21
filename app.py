from random import randint, random
from random import seed
# from flask_sqlalchemy import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session,relationship
from sqlalchemy import create_engine,Column,String,Integer,Float,Date,Table,insert
from sqlalchemy.sql.schema import ForeignKey
from flask import Flask, render_template

import requests

app = Flask(__name__)

# Global variables
namesChar = []
print('Creating database')
base_url = 'https://swapi.dev/api/'

# variables for data creation
# Create Database Connection
# ----------------------------------
database_path = "salesdb"
engine = create_engine(f"sqlite:///{database_path}")

base = declarative_base()

class Salesdb(base):
    __tablename__ = "salesdb"
    poster_content = Column(String(255),primary_key=True)
    quantity  = Column(Integer)
    price = Column(Float)
    email  = Column(String(50))
    sales_rep = Column(String(50))
    promo_code  = Column(String(10))


# variable for ETL
database_path='dw'
engine2 = create_engine(f"sqlite:///{database_path}")

base2 = declarative_base()


# starships_films = Table('starship_films', base2.metadata,
#     Column('film_id', Integer, ForeignKey('films.id')),
#     Column('starship_id', Integer, ForeignKey('starship.id'))
#     )
class Starship_films(base2):
    __tablename__ = "starship_films"
    id = Column(Integer,primary_key=True)
    film_id = Column(Integer, ForeignKey('films.id'))
    starship_id = Column(Integer, ForeignKey('starship.id'))


class Sales(base2):
    __tablename__ = "sales"
    id = Column(Integer,primary_key=True)
    starship_id = Column(Integer, ForeignKey('starship.id'))
    total = Column(Float)
    quantity = Column(Integer)
    price = Column(Float)
    promo = Column(String(10))
    sales_rep = Column(String(50))
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


@app.route('/refresh/<int:seedNum>')
def create_salesdb(seedNum):

    # Use this to clear out the db
    # ----------------------------------
    base.metadata.drop_all(engine) 
    
    # get the list of starships
    swURL = base_url + 'starships/'

    getList(swURL)

    # get the list of planets
    swURL = base_url + 'planets/'

    getList(swURL)

    # where does the character names start
    startName = len(namesChar)

    # get the list of characters
    swURL = base_url + 'people/'

    getList(swURL)

    # get the length of the array
    maxData = len(namesChar)

    # Create a "Metadata" Layer That Abstracts our SQL Database
    # ----------------------------------
    base.metadata.create_all(engine)

    # define the table and insert fake data
    session = Session(bind=engine)

    seed(seedNum)
    email = 'test@gmail.com'
    sales_rep = ''
    for i in range(100):
        count = randint(1,10)
        price = round(random() * 10,2)
        # ran = len(namesChar)/10
        poster_name = namesChar[randint(0,maxData)]

        dupNameChk = session.query(Salesdb.poster_content).filter(Salesdb.poster_content == poster_name).first()

        if dupNameChk: continue
        sales = namesChar[randint(0,maxData-startName-1) + startName]
        sales_split = sales.rsplit(' ')
        sales_rep=sales_split[0] + '@swmail.com'
        # print(poster_name,count,price,sales_rep)
        promoCode = ' '
        if randint(0,10) == 1: promoCode = 'mobile'
        try:
            new_rec = Salesdb(poster_content=poster_name,quantity=count,price=price,email=email, sales_rep=sales_rep,promo_code=promoCode)
            session.add(new_rec)
            session.commit()
        except ValueError as ex:
            print('creating salesdb failed',ex)
            session.close()
            return 'Error in creating salesDB'
            
    # session.commit()

    results = session.query(Salesdb).all()

    session.close()
    # return 'Seed database created with seed num %f' % seedNum
    return  render_template('create.html', results=results,seed=seedNum)
    

@app.route('/')
def hello():
    return 'Hello Star Wars fans!'

def getDetails(url):
    # clear out list
    starshipAr = {}

    session2 = Session(bind=engine2)

    response = requests.get(url).json()
        
    # extract results
    results = response['results']

    cond = True
    count = 0
    filmCnt = 0
    filmAssocCnt = 0
    while cond:
        for entry in results:

            count += 1

            starshipAr[entry['name']] =  count

            try:
                for film in entry['films']:
                    
                    filmAssocCnt += 1

                    results = session2.query(Films.id).filter(Films.name == film).first()

                    if results:
                        # print('match film found',int(results))
                        for filmID in results:
                            new_assoc = Starship_films(id= filmAssocCnt,film_id = filmID,
                                                   starship_id = count) 

                    else:
                        filmCnt += 1
                        new_rec = Films(id=filmCnt,
                                        name = film)
                        session2.add(new_rec)
                        new_assoc = Starship_films(id= filmAssocCnt,film_id = filmCnt, 
                                                starship_id = count )
                        
                    session2.add(new_assoc)
                    session2.commit()
            except ValueError as ex:
                print("something went wrong with the film", ex)
                break
            try:
                new_rec = Starship(id = count,
                                    name = entry['name'],
                                    url = entry['url'],
                                    created = entry['created']
                                    )
                session2.add(new_rec)
                session2.commit()
            except ValueError as ex:
                print("something went wrong with the film", ex)
                # Use this to clear out the db
                # ----------------------------------
                base2.metadata.drop_all(engine2)
                break

        if response['next'] == None: break
        response = requests.get(response['next']).json()
        # if response != 200: cond = False
        # if response.status_code != 200 : cond=False
        # extract results
        results = response['results']
        session2.close()
    return starshipAr


@app.route('/run_ETL/')
def run_ETL():
    # Use this to clear out the db
    # ----------------------------------
    base2.metadata.drop_all(engine2)

    # Create a "Metadata" Layer That Abstracts our SQL Database
    # ----------------------------------
    base2.metadata.create_all(engine2)
    
    # bind the session for accessing the salesDB
    session = Session(bind=engine)



    # get the list of starships
    swURL = base_url + 'starships/'
    # print(swURL)

    starshipAr = getDetails(swURL)
    session2 = Session(bind=engine2)
    count = 0
    # print(starshipAr)
    for instance in session.query(Salesdb).all(): 
        # Check if it is a starship 
        # print(" final loop", instance.poster_content, starshipAr.keys())
        if instance.poster_content in starshipAr.keys(): 
            
            # we have a starship poster, save it into the DW
            # call the API to return additional information. One thought would be to pull all
            # data at one time and than parse the 
            count += 1
            # print(starshipAr[instance.poster_content])
            try:
                new_rec = Sales(id = count,
                                    starship_id = starshipAr[instance.poster_content],
                                    total = round(instance.quantity * instance.price,2),
                                    quantity = instance.quantity,
                                    price = instance.price,
                                    promo = instance.promo_code,
                                    sales_rep = instance.sales_rep
                                    )
                session2.add(new_rec)
                session2.commit()
            except ValueError as ex:
                print("something went wrong with the sales", ex)
                # Use this to clear out the db
                # ----------------------------------
                base2.metadata.drop_all(engine2)
                break

    results = session2.query(Sales).all()
    stars = session2.query(Starship).all()
    film = session2.query(Films).all()
    assoc = session2.query(Starship_films).all()

    session2.close()
    session.close()
    return render_template('sale.html',results = results, stars = stars, film = film, assoc = assoc)
    # return  'ETL table created'

if __name__ == '__main__':
    app.run(debug=True)