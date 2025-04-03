import json
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
import datetime as dt
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
from pandas.plotting import table
from telegram import Bot
import nest_asyncio
import asyncio

# Get current UTC time
now = datetime.utcnow()

# If it's the 0th hour of the day, adjust for the previous day
if now.hour == 0:
    previous_day = now - timedelta(days=1)
    start_day = datetime(previous_day.year, previous_day.month, previous_day.day, 0, 0, 0)
    end_day = datetime(previous_day.year, previous_day.month, previous_day.day, 23, 59, 59)
else:
    # Start of today (00:00:00)
    start_day = datetime(now.year, now.month, now.day, 0, 0, 0)
    # End of today (23:59:59)
    end_day = datetime(now.year, now.month, now.day, 23, 59, 59)

# Format the datetime object into the desired string format
end_time = end_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')
start_time = start_day.strftime('%Y-%m-%dT%H:%M:%S.000Z')

# Define the URLs and headers for the API calls
txn_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=0&LanguageId=en'
cust_url = 'https://adminwebapi.iqsoftllc.com/api/Main/ApiRequest?TimeZone=0&LanguageId=en'

# Transaction request data
txn_data = {
    "Controller": "PaymentSystem",
    "Method": "GetPaymentRequestsPaging",
    "RequestObject": {
        "Controller": "PaymentSystem",
        "Method": "GetPaymentRequestsPaging",
        "SkipCount": 0,
        "TakeCount": 9999,
        "OrderBy": None,
        "FieldNameToOrderBy": "",
        "Type": 2,
        "HasNote": False,
        "FromDate": start_time,
        "ToDate": end_time
    },
    "UserId": "1780",
    "ApiKey": "betfoxx_api_key"
}

# Customers request data
cust_data = {
    "Controller": "Client",
    "Method": "GetClients",
    "RequestObject": {
        "Controller": "Client",
        "Method": "GetClients",
        "SkipCount": 0,
        "TakeCount": 9999,
        "OrderBy": None,
        "FieldNameToOrderBy": "",
        "CreatedFrom": start_time,
        "CreatedBefore": end_time
    },
    "UserId": "1780",
    "ApiKey": "betfoxx_api_key"
}

# Fetch transaction data
txn_response = requests.post(txn_url, json=txn_data)
txn_response_data = txn_response.json()
txn_entities = txn_response_data['ResponseObject']['PaymentRequests']['Entities']
txns = pd.DataFrame(txn_entities)

# Fetch customer data
cust_response = requests.post(cust_url, json=cust_data)
cust_response_data = cust_response.json()
cust_entities = cust_response_data['ResponseObject']['Entities']
customers = pd.DataFrame(cust_entities)

# Edge case handling for empty transaction data
if txns.empty:
    print("No deposits for current day as of:", datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

# Edge case handling for empty customer data
if customers.empty:
    print("No customer signups for current day as of:", datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'))

# Check if txns DataFrame is not empty before creating Partner_Name column
if not txns.empty:
    txns['Status'] = ['Approved' if x == 8 else 'ApprovedManually' if x == 12 else 'Cancelled' if x == 2 else
                      'CancelPending' if x == 14 else 'Confirmed' if x == 7 else 'Declined' if x == 6 else
                      'Deleted' if x == 11 else 'Expired' if x == 13 else 'Failed' if x == 9 else
                      'Frozen' if x == 4 else 'InProcess' if x == 3 else 'Pay Pending' if x == 10 else
                      'Pending' if x == 1 else 'Splitted' if x == 15 else 'Waiting For KYC' if x == 5 else 'NA'
                      for x in txns['State']]

    txns['Payment_Method'] = ['InternationalPSP' if x == 326 else 'NOWPay' if x == 147 else 'XcoinsPayCard' if x == 324
                              else 'XcoinsPayCrypto' if x == 323 else 'Omer' if x == 345 else 'PayOpPIX' if x == 160
                              else 'PayOpNeosurf' if x == 159 else 'PayOpNeosurfUK' if x == 347 else 'PayOpBankAT' if x == 352
                              else 'PayOpRevolut' if x == 161 else 'PayOPInterac' if x == 348 else 'PayOpCashToCode' if x == 350
                              else 'PayOpRevolutUK' if x == 356 else 'PayOpBankUK' if x == 353 else 'PayOpMonzo' if x == 349
                              else 'Others' for x in txns['PaymentSystemId']]

    txns['Partner_Name'] = ['Betfoxx' if x == 20 else
                            'SlotsAmigo' if x == 137 else
                            'SlotsDynamite' if x == 140 else
                            'BullSpins' if x == 147 else
                            'JawBets' if x == 149 else
                            'Unknown'
                            for x in txns['PartnerId']]
    txns['AffiliateId'] = txns['AffiliateId'].fillna('NAN')
    successful_txn = txns[txns['Status'].isin(['Approved', 'ApprovedManually'])]
    successful_txn_1 = successful_txn.groupby(['Partner_Name','AffiliateId']).agg(Deposits=('Partner_Name', 'size'),
                                                                   Deposit_Amount=('ConvertedAmount', 'sum'),
                                                                 FTDs=('DepositCount', lambda x: (x == 1).sum())).reset_index()

# Check if customers DataFrame is not empty before creating Partner_Name column
if not customers.empty:
    customers['Partner_Name'] = ['Betfoxx' if x == 20 else
                                 'SlotsAmigo' if x == 137 else
                                 'SlotsDynamite' if x == 140 else
                                 'BullSpins' if x == 147 else
                                 'JawBets' if x == 149 else
                                 'Unknown'
                                 for x in customers['PartnerId']]
    customers['AffiliateId'] = customers['AffiliateId'].fillna('NAN')
    customers_1 = customers.groupby(['Partner_Name','AffiliateId']).agg(SignUps=('Partner_Name', 'size')).reset_index()

# Merge the successful transactions with customer data
combined = successful_txn_1.merge(customers_1, on=['Partner_Name','AffiliateId'], how='outer')

# Format data
combined['SignUps'] = combined['SignUps'].apply(lambda x: f'{x:,.0f}' if pd.notna(x) else '0')
combined['FTDs'] = combined['FTDs'].apply(lambda x: f'{x:,.0f}' if pd.notna(x) else '0')
combined['Deposits'] = combined['Deposits'].apply(lambda x: f'{x:,.0f}' if pd.notna(x) else '0')
combined['Deposit_Amount'] = combined['Deposit_Amount'].apply(lambda x: f'€{x:,.0f}'.replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notna(x) else '€0')

# Split data by Partner_Name
partners = ['Betfoxx', 'SlotsAmigo', 'SlotsDynamite', 'BullSpins', 'JawBets']
tables = []

for partner in partners:
    partner_data = combined[combined['Partner_Name'] == partner]
    
    # Calculate total row
    total_row = pd.DataFrame({
        'Partner_Name': ['Total'],
        'AffiliateId': [''],
        'Deposits': [partner_data['Deposits'].astype(int).sum()],
        'Deposit_Amount': [partner_data['Deposit_Amount'].apply(lambda x: x.replace('€', '').replace(',', '').replace('.', '')).astype(float).sum()],
        'SignUps': [partner_data['SignUps'].apply(lambda x: x.replace(',', '')).astype(int).sum()],
        'FTDs': [partner_data['FTDs'].apply(lambda x: x.replace(',', '')).astype(int).sum()]
    })
    
    total_row['Deposit_Amount'] = total_row['Deposit_Amount'].apply(lambda x: f'€{x:,.0f}'.replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notna(x) else '€0')

    partner_data = pd.concat([partner_data, total_row], ignore_index=True)
    
    partner_data = partner_data[['Partner_Name','AffiliateId','SignUps','FTDs','Deposits','Deposit_Amount']]

    data = {'AffiliateId': [15,8,9,16,13,12,22,21,11,17,20,30,28,36,33,26,39],
    'Affiliate UserName': ['wabitech','fangsmedia','onlinecasinobonusppc','slimsumo','And_PC','Traffic','Kpower','ukseo22','MMDCasino22','Maisontraffic','BNWMEDIA', 'tordu92','Avstraffik','trafficj','BIZAGLO','Themediahunters','Digital-mirage']
    }

    Affs = pd.DataFrame(data)

    Affs['AffiliateId'] = Affs['AffiliateId'].astype(str)

    partner_data = partner_data.merge(Affs, on=['AffiliateId'], how='left')
    
    partner_data = partner_data[['Partner_Name','AffiliateId','Affiliate UserName','SignUps','FTDs','Deposits','Deposit_Amount']]
    

    # Plot table
    fig, ax = plt.subplots(figsize=(6, 2.5))  # Adjusted size for better readability
    ax.axis('off')
    header = f"{partner} current day summary as of {formatted_time}"
    ax.text(0.5, 1.3, header, ha='center', va='bottom', fontsize=8, fontweight='bold', color='black')
    col_widths = [0.195] * len(partner_data.columns)
    table = ax.table(cellText=partner_data.values, colLabels=partner_data.columns, loc='center', cellLoc='center', colWidths=col_widths)
    
    # Disable auto font size and set it explicitly
    table.auto_set_font_size(False)
    table.set_fontsize(8)

    # Set padding and adjust cell properties
    for (i, j), cell in table.get_celld().items():
        cell.set_fontsize(8)
        cell.set_text_props(ha='center', va='center')
        cell.set_edgecolor('black')
        cell.set_linewidth(1)
        if i == 0:  # Set header row background color
            cell.set_facecolor('#e6e6e6')

        if i == len(partner_data):  # Total row
            cell.set_text_props(weight='bold')
            cell.set_facecolor('#e6e6e6')

    # Adjust layout
    plt.subplots_adjust(left=0.05, right=0.95, top=0.7, bottom=0.05)
    filename = f'table_snapshot_{partner}.png'
    plt.savefig(filename, bbox_inches='tight', dpi=300)
    plt.close()

    # Add to table list
    tables.append(filename)

# Send tables via Telegram
nest_asyncio.apply()

TOKEN = '8136460878:AAFvL8CYVaAnZx7srn8Yuwy0HQkERtLZlDc'
CHAT_ID = '-4524311273'

async def send_tables():
    bot = Bot(token=TOKEN)

    for table_filename in tables:
        with open(table_filename, 'rb') as photo:
            await bot.send_photo(chat_id=CHAT_ID, photo=photo)
        print(f"{table_filename} sent to Telegram!")

# Run the send_tables function asynchronously
asyncio.run(send_tables())
