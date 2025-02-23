import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

def send_email(subject, body, recipient_emails: dict[str, str]):
    sender_email = "your_email@gmail.com"
    sender_password = "your_app_password"

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = recipient_emails
    message["Subject"] = subject
    message.attach(MIMEText(body, "plain"))

    try:
        context = ssl.create_default_context()
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient_email, message.as_string())
        print("Email sent successfully!")
    except Exception as e:
        print(f"Error: {e}")

# send_email("Test Subject", "This is a test email.", "recipient@example.com")
# TODO: Integrate this function with the rest of the codebase to send email notifications.
