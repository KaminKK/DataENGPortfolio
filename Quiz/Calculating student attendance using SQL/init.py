import pandas as pd

# URL of the CSV file on GitHub
url = "https://raw.githubusercontent.com/erood/interviewqs.com_code_snippets/master/Datasets/ddi_message_app_data.csv"

# Read the CSV file into a DataFrame
df = pd.read_csv(url)

# Assuming you have already loaded the data into a DataFrame named df

# Convert the 'date' column to a datetime object
df['date'] = pd.to_datetime(df['date'])

# Filter the data for messages sent on March 1, 2018
march_1_data = df[df['date'] == '2018-03-01']

# Group by 'sender_id' and count the unique 'receiver_id' for each sender
unique_receiver_counts = march_1_data.groupby('sender_id')['receiver_id'].nunique()

# Filter senders with at least 9 unique receivers
senders_with_at_least_9_unique_receivers = unique_receiver_counts[unique_receiver_counts >= 9]

# Calculate the fraction of such senders
fraction = len(senders_with_at_least_9_unique_receivers) / len(df['sender_id'].unique())

print(f"The fraction of senders who sent messages to at least 9 unique people on March 1, 2018 is: {fraction:.2%}")
