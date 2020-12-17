#If crontab is working, it will send email to tradingnotification.1@gmail.com

import smtplib, ssl, config
context = ssl.create_default_context()

message = "I, crontab-e is working"

with smtplib.SMTP_SSL(config.EMAIL_HOST, config.EMAIL_PORT, context=context) as server:
    server.login(config.EMAIL_ADDRESS, config.EMAIL_PASSWORD)
    email_message = f"Subject: Crontab Check\n\n"
    email_message += "\n\n".join(message)
    server.sendmail(config.EMAIL_ADDRESS, config.EMAIL_ADDRESS, email_message)
