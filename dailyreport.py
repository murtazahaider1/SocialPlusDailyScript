###########################################################

# This cron job runs the SocialPlus daily report script automatically.
# Server Time Zone: China Standard Time (CST)
# Execution Time: 03:05 AM CST (equivalent to 12:05 AM Pakistan Time - PKT)
# The script runs using the Python environment located at:
# /home/ubuntu/socialplus_report_job/.env/bin/python
# 
# Command:
# 5 3 * * * /home/ubuntu/socialplus_report_job/.env/bin/python /home/ubuntu/socialplus_report_job/SocialPlusDailyScript/dailyreport.py >> /home/ubuntu/socialplus_report_job/SocialPlusDailyScript/cron.log 2>&1
#
# Output and errors are logged in cron.log for debugging and monitoring.

###########################################################
import psycopg2
import csv
from datetime import date
import configparser
from datetime import date, timedelta
import logging
import os

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)


current_path = os.path.dirname(os.path.realpath(__file__))

config = configparser.ConfigParser()
config.read(f"{current_path}/config.ini")  

host = config.get("database", "host")
port = config.get("database", "port")
db_name = config.get("database", "db_name")
user = config.get("database", "user")
password = config.get("database", "password")


yesterday = date.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
file_name = f"{current_path}/socialplus_{yesterday_str}.csv"
print(yesterday_str)

file_handler = logging.FileHandler(f"{current_path}/socialpluslog_{yesterday_str}.log")
file_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)
logging.getLogger().addHandler(file_handler)

try:
    # Connect to the db
    connection = psycopg2.connect(
        host=host,
        port=port,
        database=db_name,
        user=user,
        password=password
    )

    logging.info("Connecting to the database\n")
    print("Connected to the database")

    curr = connection.cursor()

    # Set the correct schema to simosa_feed
    curr.execute("SET search_path TO simosa_feed;")
    logging.info("Schema set to simosa_feed\n")
    print("Schema set to simosa_feed\n")
    curr.execute("SET timezone To 'GMT-5';")
    logging.info("Timezone Set to GMT +5")

    connection.set_client_encoding('UTF8')

    queries = [
        (
            "Users Created",
            f"""SELECT COUNT(*) As "Users Created"
            FROM simosa_feed.users 
            WHERE DATE(created_at) = '{yesterday_str}';"""
        ),
        (
            "Posts",
            f"SELECT COUNT(*) FROM simosa_feed.posts WHERE DATE(created_at) = '{yesterday_str}';"
        ),
        (
            "Group Following",
            f"""SELECT Count(*) AS total_followers
            FROM simosa_feed.groups g  
            JOIN simosa_feed.group_followers n ON n.group_id = g.id
            Join simosa_feed.users u ON n.user_id=u.id
            Where DATE(n.followed_at) = '{yesterday_str}';"""

        ),
        (
            "1-1 Following (Total)",
            f"""select count(*) from simosa_feed.followers;"""
        ),
        (
            "Comments",
            f"""select count(*) from comments where date(created_at)='{yesterday_str}';"""
        ),
        (
            "Likes",
            f"""select count(user_id) from post_likes where date(created_at)='{yesterday_str}';"""
        ),
        (
            "Users Liked",
            f"""select count(distinct(user_id)) from post_likes where date(created_at)='{yesterday_str}';"""
        ),
        (
            "Users Commented",
            f"""select count(distinct(user_id)) from comments where date(created_at)='{yesterday_str}';"""
        ),
        (
            "Active Users",
            f"""with active_users as (
	        -- Users Created
	        select distinct(t.id) as user_id from simosa_feed.users t where t.created_at::date = '{yesterday_str}'
	        union
	        -- Posts
	        select distinct(t.user_id) as user_id from simosa_feed.posts t where t.created_at::date = '{yesterday_str}'
	        union
	        -- Group Following
	        select distinct(t.user_id) as user_id from simosa_feed.group_followers t where t.followed_at::date = '{yesterday_str}'
	        union
	        -- Users Liked
	        select distinct(t.user_id) as user_id from simosa_feed.post_likes t where t.created_at::date = '{yesterday_str}'
	        union
	        -- Users Commented
	        select distinct(t.user_id) as user_id from simosa_feed.comments t where t.created_at::date = '{yesterday_str}'
            )
            select count(t.user_id) from active_users t;"""
        )
    ]

    # Function to check if each query returns a single output or more (Not needed for updated Queries)
    def process_result(rows):
        if not rows:
            return ""
        if len(rows) == 1 and len(rows[0]) == 1:
            return rows[0][0]
        else:
            return "; ".join([": ".join(map(str, row)) for row in rows])

    # Each query stored in Dictionary
    results = {}
    for header, query in queries:
        curr.execute(query)
        rows = curr.fetchall()
        results[header] = process_result(rows)


    with open(file_name, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        header_row = ["Date"] + [header for header, _ in queries]
        writer.writerow(header_row)
        data_row = [yesterday_str] + [results[header] for header, _ in queries]
        writer.writerow(data_row)

    logging.info(f"Results saved to {file_name}")
    print(f"Results saved to {file_name}")

    # Close connection
    curr.close()
    connection.close()
    logging.info("Connection Closed")
    print("Connection closed")

except Exception as e:
    logging.Exception("Error connecting to the database: %s", e)
    print("Error connecting to the database:", e)

# Sending Mail
import smtplib
from email.message import EmailMessage

def send_email_with_csv(attachment_file, today_str, sender_email, sender_password, receiver_email):
    msg = EmailMessage()
    msg["Subject"] = f"Social+ Daily Report for {today_str}"
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg.set_content("Hi Team,\n\nPlease find attached the daily report CSV file for Social+.\n\nThank you")

    with open(attachment_file, "rb") as f:
        file_data = f.read()
        file_name = attachment_file.split("/")[-1]
    msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
          server.starttls()
          server.login(sender_email, sender_password)
          server.send_message(msg)
          logging.info(f"Email sent to: {receiver_emails_str}" )
          print(f"Email sent to:  {receiver_emails_str}")
    except Exception as e:
        logging.Exception("Error sending email", e)
        print("Error sending email:", e)

if __name__ == "__main__":
    yesterday = date.today() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    file_name = f"{current_path}/socialplus_{yesterday_str}.csv"
    sender_email = config.get("email", "sender_email")
    sender_password = config.get("email", "sender_password")
    #receiver_email = config.get("email", "receiver_email")
    receiver_emails_str = config.get("email", "receiver_email")
    receiver_emails = [email.strip() for email in receiver_emails_str.split(",")]


    send_email_with_csv(file_name, yesterday_str, sender_email, sender_password, receiver_emails)
