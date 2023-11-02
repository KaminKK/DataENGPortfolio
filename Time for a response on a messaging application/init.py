import pandas as pd

# Load your dataset into a pandas DataFrame (assuming it's in a CSV file)
data = pd.read_csv("./sample_message_dataset.csv")

# Convert the 'timestamp' column to datetime format
data['timestamp'] = pd.to_datetime(data['timestamp'], unit='s')

# Sort the DataFrame by 'sender_id', 'receiver_id', and 'timestamp'
data.sort_values(by=['sender_id', 'receiver_id', 'timestamp'], inplace=True)

# Calculate the time difference between consecutive messages for the same sender and receiver
data['time_diff'] = data.groupby(['sender_id', 'receiver_id'])['timestamp'].diff()

# Calculate the fraction of messages with a time difference less than 5 minutes
within_5_minutes = (data['time_diff'] <= pd.Timedelta(minutes=5)).sum()
total_messages = len(data)

fraction_within_5_minutes = within_5_minutes / total_messages

print(f"Fraction of messages sent between the same sender and receiver within 5 minutes: {fraction_within_5_minutes:.2%}")
