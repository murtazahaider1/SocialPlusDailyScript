import psycopg2
import csv
from datetime import date
import configparser
from datetime import date, timedelta
import logging

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s'
)



config = configparser.ConfigParser()
config.read('config.ini')  

host = config.get("database", "host")
port = config.get("database", "port")
db_name = config.get("database", "db_name")
user = config.get("database", "user")
password = config.get("database", "password")


yesterday = date.today() - timedelta(days=1)
yesterday_str = yesterday.strftime("%Y-%m-%d")
file_name = f"socialplus_{yesterday_str}.csv"
print(yesterday_str)

file_handler = logging.FileHandler(f"socialpluslog_{yesterday_str}.log")
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
    curr.execute("SET timezone To 'Asia/Karachi';")
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
            f"""Select count((f.follower_id)) as Followers 
            from simosa_feed.users u 
            join simosa_feed.followers f on u.id=f.user_id  where is_active != False;"""
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
    file_name = f"socialplus_{yesterday_str}.csv"
    sender_email = config.get("email", "sender_email")
    sender_password = config.get("email", "sender_password")
    #receiver_email = config.get("email", "receiver_email")
    receiver_emails_str = config.get("email", "receiver_email")
    receiver_emails = [email.strip() for email in receiver_emails_str.split(",")]


    send_email_with_csv(file_name, yesterday_str, sender_email, sender_password, receiver_emails)