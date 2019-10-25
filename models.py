from sqlalchemy import Column, Integer, Text, DateTime, Boolean, ForeignKey, String
from sqlalchemy.dialects.postgresql import ENUM, MONEY
from sqlalchemy.ext.declarative import declarative_base

import utils

Base = declarative_base()


AUTH_TYPE = ENUM('internal', 'google_cloud', 'aws', 'azure', 'facebook', name='AUTH_TYPE')
BID_STATUS = ENUM('active', 'inactive', 'winner', 'loser', name='BID_STATUS')
LISTING_STATUS = ENUM('open', 'closed', 'expired', 'failed_payment', 'successful_payment', name='LISTING_STATUS')
PAYMENT_METHOD = ENUM('credit_card', 'payu', 'paypal', 'bitcoin', name='PAYMENT_METHOD')


class User(Base):
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64))
    auth_type = Column(AUTH_TYPE)
    auth_id = Column(Text)
    created_at = Column(DateTime)
    last_login = Column(DateTime)
    active = Column(Boolean)


class Listing(Base):
    __tablename__ = 'listings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    seller_id = Column(Integer, ForeignKey('users.id'))
    description = Column(Text)
    minimal_price = Column(MONEY)
    opening_time = Column(DateTime)
    closing_time = Column(DateTime)
    status = Column(LISTING_STATUS)


class Picture(Base):
    __tablename__ = 'pictures'

    id = Column(Integer, primary_key=True, autoincrement=True)
    description = Column(Text)
    storage_key = Column(Text)
    width = Column(Integer)
    height = Column(Integer)
    upload_time = Column(DateTime)


class ListingPicture(Base):
    __tablename__ = 'listings_pictures'

    id = Column(Integer, primary_key=True, autoincrement=True)
    listing_id = Column(Integer, ForeignKey('listings.id'))
    picture_id = Column(Integer, ForeignKey('pictures.id'))


class Bid(Base):
    __tablename__ = 'bids'

    id = Column(Integer, primary_key=True, autoincrement=True)
    listing_id = Column(Integer, ForeignKey('listings.id'))
    bidder_id = Column(Integer, ForeignKey('users.id'))
    bid_price = Column(MONEY)
    bid_time = Column(DateTime)
    bid_status = Column(BID_STATUS)


class Sale(Base):
    __tablename__ = 'sales'

    id = Column(Integer, primary_key=True, autoincrement=True)
    bid_id = Column(Integer, ForeignKey('bids.id'))
    listing_id = Column(Integer, ForeignKey('listings.id'))
    sale_price = Column(MONEY)
    marketplace_brokerage = Column(MONEY)
    payment_deadline = Column(DateTime)
    payment_time = Column(DateTime)
    payment_method = Column(PAYMENT_METHOD)
    payment_transaction_id = Column(Text)


# ==== Insert new models ABOVE this comment
if __name__ == '__main__':
    Base.metadata.drop_all(utils.engine)
    Base.metadata.create_all(utils.engine)
    utils.session.commit()

    # b = Bid(bid_price='12.34')
    # utils.session.add(b)
    # utils.session.commit()

# import factory
#
# class UserFactory(factory.alchemy.SQLAlchemyModelFactory):
#     class Meta:
#         model = User
#         sqlalchemy_session = session   # the SQLAlchemy session object
#
#     id = factory.Sequence(lambda n: n)
#     name = factory.Sequence(lambda n: u'User %d' % n)