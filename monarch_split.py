import asyncio
import yaml
import polars as pl
import dotenv
import os
from datetime import date, timedelta
from monarchmoney import MonarchMoney


async def split():

    dotenv.load_dotenv()

    mm = MonarchMoney()
    print("Logging in...", end = "")
    await mm.login(
        email = os.getenv("EMAIL"), 
        password = os.getenv("PASSWORD"),
        mfa_secret_key = os.getenv("MFA")
    )
    print("done.")

    print("Getting transactions and categories...", end = "")
    cats = await mm.get_transaction_categories()
    cat_lookup = {cat['name']: cat['id'] for cat in cats['categories']}

    with open("splits.yaml") as f:
        splits = yaml.safe_load(f)

    # set the start date to the previous month
    now = date.today()
    start_date = ((now.replace(day=1) - timedelta(days=1)).replace(day=10)).strftime("%Y-%m-%d")
    end_date = now.replace(day=5).strftime("%Y-%m-%d")

    transactions = await mm.get_transactions(
       start_date = start_date,
       end_date = end_date,
       category_ids = [cat_lookup[comp['category']] for comp in splits['components']],
       has_notes = False
    )
    print("done.")

    print("Calculating splits...", end = "")
    transactions = pl.from_dicts([
        {
            'amount': transaction['amount'],
            'category': transaction['category']['name'],
            'categoryId': transaction['category']['id'],
            'merchant': transaction['merchant']['name']
        }
        for transaction in transactions['allTransactions']['results']
    ]).join(
        pl.from_dicts(splits['components']), on = ['category', 'merchant']
    ).with_columns(
        (abs(pl.col('amount')) * pl.col('rate')).alias('amount'),
        pl.col('merchant').alias('merchantName')
    )

    found_cats = set(transactions['category'])
    configured_cats = set([split['category'] for split in splits['components']])

    if found_cats != configured_cats:
        print("not all configured categories are found, quitting.")
        return()
    else:
        print("done.")

    print("Getting main expense to split...", end = "")
    try:
        accounts = await mm.get_accounts()
        venmo_account = [acc for acc in accounts['accounts'] if acc['displayName'] == "Venmo"]
        venmo_account_id = venmo_account[0]['id']
        print("done.")
    except Exception as e:
        print("Venmo account can't be found.")
        return()

    print("Applying splits...", end = "")
    venmo_expenses = await mm.get_transactions(
        start_date = start_date,
        end_date = end_date,
        account_ids = [venmo_account_id],
        has_notes=True
    )

    main_expense = [
        expense for expense in venmo_expenses['allTransactions']['results']
        if expense['notes'] == splits['notes'] and expense['amount'] > splits['threshold'] and expense['account']
    ]

    if len(main_expense) > 0:

        main_expense = main_expense[0]

        leftover_split = splits['leftover']
        leftover_split['amount'] =  main_expense['amount'] - transactions['amount'].sum()
        leftover_split['categoryId'] = cat_lookup[leftover_split['category']]
        leftover_split.pop('category')

        final_splits = transactions[['categoryId', 'merchantName', 'amount']].to_dicts() + [leftover_split]

        try:
            await mm.update_transaction(
                transaction_id = main_expense['id'],
                notes = "Split by script!"
            )
            await mm.update_transaction_splits(
                transaction_id = main_expense['id'],
                split_data = final_splits
            )
            print("done.")
        except Exception as e:
            print(f"failed to update splits: {e}")

    else:
        print("no transaction meets the criteria.")

if __name__ == "__main__":
    asyncio.run(split())