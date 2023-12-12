**Question**
---

 Using Python, calculate what fraction of senders sent messages to at least 9 unique people on March 1, 2018. 


 The table contains send/receive message data for the application's users. The structure is as follows:


 Table name: user_messaging
    
1. date
1. sender_id (#id of the message sender)
1. receiver_id (#id of the message receiver)


**Solution**
---
1. Create DataFrame
1. Cleansing (if necessary)
1. Create filter the data for message sent on 2018-03-01
1. Create count of unique reciever
1. Filter senders with at least 9 unique receivers
1. Calculate fraction of sender