"""this is an email utility"""
import os
import smtplib
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def sendMail(fromEmail, subject, body, recipients, bcc=None, port=25,
             host="postfix-proxy.devint.gcp.openx.org"):
    """
    """
    if isinstance(recipients, str):
        recipients = recipients.split(',')
    if bcc:
        if isinstance(bcc, str):
            bcc = bcc.split(',')
    else:
        bcc = []
    msg = MIMEMultipart()
    msg.attach(MIMEText(body, 'html'))
    msg["Subject"] = subject if subject else ""
    msg["From"] = fromEmail
    msg['To'] = ",".join(recipients)

    # we can now slap in bcc emails without revealing them to recipients
    recipients = recipients + bcc

    # setup server and send email
    server = smtplib.SMTP(host, port)
    server.sendmail(fromEmail, recipients, msg.as_string())
    server.close()