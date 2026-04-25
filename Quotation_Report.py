import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import smtplib
from email.message import EmailMessage
import logging
import schedule
import time
import os
import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

os.makedirs("reports", exist_ok=True)

DB_CONFIG = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=125.22.246.228;"
    "DATABASE=GCT;"
    "UID=Intern;"
    "PWD=Welcome@123!;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
)

SENDER_EMAIL = "thomasbenita18@gmail.com"
APP_PASSWORD = "nmucfkhymqofqykf"
TO_EMAILS = ["prabu@seaknots.in"]
CC_EMAILS = ["nanda@seaknots.in", "kuppu@seaknots.in", "releaseengineer@seaknots.in"]


def connect_to_database():
    logging.info("Connecting to SQL Server...")
    conn = pyodbc.connect(DB_CONFIG)
    logging.info("Connected successfully.")
    return conn


def fetch_data(conn):
    logging.info("Fetching quotation data...")
    query = "EXEC sp_ReportQuotation @FilterString='', @userId=1"
    df = pd.read_sql(query, conn)
    logging.info(f"Fetched {len(df)} rows.")
    return df


def generate_charts(df, timestamp):
    chart_files = []

    if 'QStus' in df.columns:
        chart_path = os.path.join("reports", f"quotation_status_chart_{timestamp}.png")
        df['QStus'].value_counts().plot(kind='bar', color='steelblue')
        plt.title('Quotation Status Count')
        plt.xlabel('Status')
        plt.ylabel('Count')
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close()
        chart_files.append(chart_path)
        logging.info(f"Chart saved: {chart_path}")

    if 'Client' in df.columns:
        chart_path = os.path.join("reports", f"top_customers_chart_{timestamp}.png")
        df['Client'].value_counts().head(10).plot(kind='bar', color='darkorange')
        plt.title('Top 10 Customers')
        plt.xlabel('Customer')
        plt.ylabel('Count')
        plt.tight_layout()
        plt.savefig(chart_path)
        plt.close()
        chart_files.append(chart_path)
        logging.info(f"Chart saved: {chart_path}")

    return chart_files


def export_to_excel(df, timestamp):
    excel_path = os.path.join("reports", f"quotation_report_{timestamp}.xlsx")
    df.to_excel(excel_path, index=False)
    logging.info(f"Excel report saved: {excel_path}")
    return excel_path


def build_email(excel_file, chart_files):
    msg = EmailMessage()
    msg["Subject"] = "Automated SQL Quotation Report"
    msg["From"] = SENDER_EMAIL
    msg["To"] = ", ".join(TO_EMAILS)
    msg["Cc"] = ", ".join(CC_EMAILS)
    msg.set_content(
        "Hi,\n\n"
        "Please find attached the latest automated quotation report along with the summary charts.\n\n"
        "This report is generated automatically every 20 minutes.\n\n"
        "Regards,\nBenita"
    )

    with open(excel_file, "rb") as f:
        msg.add_attachment(
            f.read(),
            maintype="application",
            subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            filename=os.path.basename(excel_file)
        )

    for chart in chart_files:
        with open(chart, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="image",
                subtype="png",
                filename=os.path.basename(chart)
            )

    return msg


def send_email(msg):
    all_recipients = TO_EMAILS + CC_EMAILS
    logging.info("Sending email...")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SENDER_EMAIL, APP_PASSWORD)
        server.send_message(msg, to_addrs=all_recipients)
    logging.info("Email sent successfully.")


def generate_and_send_report():
    try:
        logging.info("Report job started.")
        conn = connect_to_database()
        df = fetch_data(conn)
        conn.close()

        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        chart_files = generate_charts(df, timestamp)
        excel_file = export_to_excel(df, timestamp)

        msg = build_email(excel_file, chart_files)
        send_email(msg)

        logging.info("Report job completed successfully.")

    except Exception as e:
        logging.error(f"Report job failed: {e}")


logging.info("Starting report automation...")
generate_and_send_report()

schedule.every(20).minutes.do(generate_and_send_report)
logging.info("Scheduler running. Next report in 20 minutes...")

while True:
    schedule.run_pending()
    time.sleep(60)
