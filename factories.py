import random
from datetime import timedelta

import factory
from factory.alchemy import SQLAlchemyModelFactory
from faker import Faker

import models
import utils

fake = Faker()


class UserFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.User
        sqlalchemy_session = utils.session

    name = factory.Faker('ascii_email')
    auth_type = factory.LazyFunction(lambda: fake.random_element(models.AUTH_TYPE.enums))
    auth_id = factory.Faker('sha256')
    created_at = factory.Faker('date_time_between', start_date='-5y', end_date='-1y')
    last_login = factory.Faker('date_time_between', start_date='-1y', end_date='now')
    active = factory.Faker('boolean')


class ListingFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.Listing
        sqlalchemy_session = utils.session

    description = factory.Faker('paragraphs', nb=3)
    minimal_price = factory.LazyFunction(
        lambda: f'{fake.pyint(min_value=5, max_value=10)}.{fake.pyint(min_value=0, max_value=99)}'
    )
    opening_time = factory.Faker('date_time_between', start_date='-1y', end_date='-6m')
    closing_time = factory.Faker('date_time_between', start_date='-6m', end_date='now')
    status = factory.LazyFunction(lambda: fake.random_element(models.LISTING_STATUS.enums))


class BidFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.Bid
        sqlalchemy_session = utils.session

    bid_price = factory.LazyFunction(
        lambda: f'{fake.pyint(min_value=11, max_value=20000)}.{fake.pyint(min_value=0, max_value=99)}'
    )


class PictureFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.Picture
        sqlalchemy_session = utils.session

    description = factory.Faker('catch_phrase')
    storage_key = factory.Faker('sha256')
    width = factory.Faker('pyint', min_value=720, max_value=2160)
    height = factory.Faker('pyint', min_value=720, max_value=2160)
    upload_time = factory.Faker('date_time_between', start_date='-7y', end_date='now')


class SaleFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.Sale
        sqlalchemy_session = utils.session

    payment_method = factory.LazyFunction(
        lambda: fake.random_element(models.PAYMENT_METHOD.enums)
    )
    payment_transaction_id = factory.Faker('ean13')


class ListingPictureFactory(SQLAlchemyModelFactory):
    class Meta:
        model = models.ListingPicture
        sqlalchemy_session = utils.session


if __name__ == '__main__':
    NUM_USERS = 1000
    NUM_PICTURES = 1000
    NUM_LISTINGS = 1000
    MIN_BIDS = 10
    MAX_BIDS = 100
    MIN_PICS_PER_LISTING = 2
    MAX_PICS_PER_LISTING = 10

    users = UserFactory.build_batch(size=NUM_USERS)
    utils.session.bulk_save_objects(users)
    utils.session.commit()
    users = {u.id: u for u in users}
    pictures = PictureFactory.build_batch(size=NUM_PICTURES)
    utils.session.bulk_save_objects(pictures)
    utils.session.commit()
    all_user_ids = set(users.keys())
    listings = ListingFactory.build_batch(
        size=NUM_LISTINGS,
        seller_id=factory.LazyFunction(
            lambda: fake.random_element(all_user_ids)
        )
    )
    utils.session.bulk_save_objects(listings)
    utils.session.commit()
    for l in listings:
        user_ids = all_user_ids - {l.seller_id}
        bids_size = random.randint(MIN_BIDS, MAX_BIDS)
        bidders = random.choices([x for x in users.values()], k=bids_size)
        bidder_gen1 = (x for x in bidders)
        bidder_gen2 = (x for x in bidders)
        bids = BidFactory.build_batch(
            size=random.randint(MIN_BIDS, bids_size),
            listing_id=l.id,
            bidder_id=factory.LazyFunction(
                lambda: next(bidder_gen1).id
            ),
            bid_time=l.opening_time + timedelta(
                seconds=random.randint(
                    1,
                    (l.opening_time - l.closing_time).seconds
                )
            ),
            bid_status='active' if next(bidder_gen2).active else 'inactive'
        )
        utils.session.bulk_save_objects(bids)
        utils.session.commit()
        winning_bid = max(bids, key=lambda x: int(x.bid_price.replace(',', '').replace('$', '').replace('.', '')))
        price_int = int(winning_bid.bid_price.replace(',', '').replace('$', '').replace('.', ''))
        sale_price = int(price_int * 0.9)
        marketplace_brokerage = price_int - sale_price
        sale_price = f'{sale_price//100}.{str(sale_price%100).zfill(2)}'
        marketplace_brokerage = f'{marketplace_brokerage//100}.{str(marketplace_brokerage%100).zfill(2)}'
        SaleFactory(
            bid_id=winning_bid.id,
            listing_id=l.id,
            sale_price=sale_price,
            marketplace_brokerage=marketplace_brokerage,
            payment_deadline=l.closing_time + timedelta(
                seconds=random.randint(7200, 14400)
            ),
            payment_time=l.closing_time + timedelta(
                seconds=random.randint(3600, 7200)
            )
        )
        winning_bid.bid_status = 'winner'
        utils.session.add(winning_bid)
    utils.session.commit()
    for l in listings:
        for p in random.choices(pictures, k=random.randint(MIN_PICS_PER_LISTING, MAX_PICS_PER_LISTING)):
            ListingPictureFactory(listing_id=l.id, picture_id=p.id)
    utils.session.commit()
