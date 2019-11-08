# here should go transactions SQL

TRANSACTION_1 = """
BEGIN TRANSACTION;

SELECT * FROM public.users;

SELECT * FROM pg_database;
COMMIT;
"""