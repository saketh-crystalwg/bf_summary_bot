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
    # Directly create the Partner_Name column based on Payment_Method after processing
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

    # Directly create 'Partner_Name' column based on predefined mapping of PartnerId
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
    
    successful_txn_2 = successful_txn_1[successful_txn_1['Partner_Name'] ==  'Betfoxx']

# Check if customers DataFrame is not empty before creating Partner_Name column
if not customers.empty:
    # Directly create 'Partner_Name' column based on predefined mapping of PartnerId
    customers['Partner_Name'] = ['Betfoxx' if x == 20 else
                                 'SlotsAmigo' if x == 137 else
                                 'SlotsDynamite' if x == 140 else
                                 'BullSpins' if x == 147 else
                                 'JawBets' if x == 149 else
                                 'Unknown'
                                 for x in customers['PartnerId']]
    
    customers['AffiliateId'] = customers['AffiliateId'].fillna('NAN')

    customers_1 = customers.groupby(['Partner_Name','AffiliateId']).agg(SignUps=('Partner_Name', 'size')).reset_index()
    
    customers_2 = customers_1[customers_1['Partner_Name'] ==  'Betfoxx']

if not txns.empty and not customers.empty:
    # Merge the successful transactions with customer data
    combined = successful_txn_2.merge(customers_2, on=['Partner_Name','AffiliateId'], how='outer')
    
    combined = combined.sort_values(by='Deposit_Amount', ascending=False)
    
    data = {
    'AffiliateId': [15,8,9,16,13,12,22,21,11,17,20,30,28,36,33,26,39],
    'Affiliate UserName': ['wabitech','fangsmedia','onlinecasinobonusppc','slimsumo','And_PC','Traffic','Kpower','ukseo22','MMDCasino22','Maisontraffic','BNWMEDIA', 'tordu92','Avstraffik','trafficj','BIZAGLO','Themediahunters','Digital-mirage']
    }

    Affs = pd.DataFrame(data)

    Affs['AffiliateId'] = Affs['AffiliateId'].astype(str)

    combined_2 = combined.merge(Affs, on=['AffiliateId'], how='left')
    
    combined_3 = combined_2[['Partner_Name','AffiliateId','Affiliate UserName','SignUps','FTDs','Deposits','Deposit_Amount']]


    # Calculate total row
    total_row = pd.DataFrame({'Partner_Name': ['Total'],'AffiliateId': [''],'Affiliate UserName': [''], 'Deposits': [combined_3['Deposits'].sum()],
                              'Deposit_Amount': [combined_3['Deposit_Amount'].sum()], 'SignUps': [combined_3['SignUps'].sum()],
                              'FTDs': [combined_3['FTDs'].sum()]})
    combined_4 = pd.concat([combined_3, total_row], ignore_index=True)

    # Format data
    combined_4['SignUps'] = combined_4['SignUps'].apply(lambda x: f'{x:,.0f}')
    combined_4['FTDs'] = combined_4['FTDs'].apply(lambda x: f'{x:,.0f}')
    combined_4['Deposits'] = combined_4['Deposits'].apply(lambda x: f'{x:,.0f}')
    combined_4['Deposit_Amount'] = combined_4['Deposit_Amount'].apply(lambda x: f'€{x:,.0f}'.replace(',', 'X').replace('.', ',').replace('X', '.'))

    combined_5 = combined_4[['Partner_Name','AffiliateId','Affiliate UserName','SignUps','FTDs','Deposits','Deposit_Amount']]
    
# Sample DataFrame
        
    
    if now.second != 0 or now.microsecond != 0:
        rounded_time = (now + timedelta(minutes=1)).replace(second=0, microsecond=0)
    else:
        rounded_time = now.replace(second=0, microsecond=0)

    # Format the rounded time to include in the header
    formatted_time = rounded_time.strftime('%Y-%m-%d %H:%M')

    # Create the header message
    header = f"BF current day summary as of {formatted_time}"

    
    fig, ax = plt.subplots(figsize=(6, 2.5))  # Adjusted size for better readability
    ax.axis('off')  # Turn off axis
    ax.text(0.5, 1.3, header, ha='center', va='bottom', fontsize=8, fontweight='bold', color='black')
    col_widths = [0.195] * len(combined_5.columns)
    table = ax.table(cellText=combined_5.values, colLabels=combined_5.columns, loc='center', cellLoc='center', colWidths=col_widths)

    # Disable auto font size and set it explicitly
    table.auto_set_font_size(False)  # Disable auto font size
    table.set_fontsize(8)  # Set font size manually for the table cells
    
    # Set padding and adjust cell properties
    for (i, j), cell in table.get_celld().items():
        cell.set_fontsize(8)  # Set font size for text inside the cell
        cell.set_text_props(ha='center', va='center')  # Center text inside cells
        cell.set_edgecolor('black')  # Set cell borders
        cell.set_linewidth(1)  # Set cell border thickness
        if i == 0:  # Set the header row's background color
            cell.set_facecolor('#e6e6e6')

    # Bold the "Total" row (last row)
        if i == len(combined_5):  # This is the last row, the "Total" row
            cell.set_text_props(weight='bold')  # Make the text bold
            cell.set_facecolor('#e6e6e6')  # Set a background color for the total row to differentiate
    # Adjust layout to minimize space between header and table
    plt.subplots_adjust(left=0.05, right=0.95, top=0.7, bottom=0.05)  # Adjusted layout for closer alignment
    # Save the image
    plt.savefig('table_snapshot.png', bbox_inches='tight', dpi=300)
    plt.close()

else:
    # If either txns or customers are empty, send appropriate message
    print("Either no deposits or no customers for the current day.")

nest_asyncio.apply()

TOKEN = '8156463627:AAHgELhXVteugJysMcYBM2Ht7C-RWS-WHUU'
CHAT_ID = '-4679957197'

# Check if combined_1 is empty or not
if combined_5.empty:
    message = f"No Transactions or Signups done for the current day as of {formatted_time}"
else:
    message = None

# Function to send the photo asynchronously
async def send_photo():
    bot = Bot(token=TOKEN)

    if message:  # If the message is set (i.e., combined_1 is empty)
        await bot.send_message(chat_id=CHAT_ID, text=message)
        print("Message sent to Telegram!")
    else:
        with open('table_snapshot.png', 'rb') as photo:
            # Send the photo asynchronously
            await bot.send_photo(chat_id=CHAT_ID, photo=photo)
        print("Table snapshot sent to Telegram!")

# Run the send_photo function asynchronously
asyncio.run(send_photo())
