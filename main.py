from fastapi import FastAPI, UploadFile, File, Form, HTTPException
import pandas as pd
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import io
import uuid
import socket
from email.utils import formatdate

app = FastAPI(title="UAFIC Email Microservice")

DEFAULT_BODY = """Добрий день, {Name}!

Мене звати Юрій, турбую Вас з Української асоціації фінтех...
(API Test - Refactored for DevOps Pipeline)
"""


@app.post("/send-batch")
async def send_batch(
        file: UploadFile = File(...),
        smtp_email: str = Form(...),
        smtp_password: str = Form(...),
        smtp_server: str = Form("mx1.mirohost.net"),
        smtp_port: int = Form(465),
        subject: str = Form("УАФІК DoWithUA - Open API platform")
):
    # 1. Read the uploaded Excel file into memory
    contents = await file.read()
    try:
        df = pd.read_excel(io.BytesIO(contents))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid Excel file: {e}")

    if 'Email' not in df.columns:
        raise HTTPException(status_code=400, detail="Missing 'Email' column in Excel")

    # 2. Connect to SMTP Server
    try:
        server = smtplib.SMTP_SSL(smtp_server, smtp_port)
        server.login(smtp_email, smtp_password)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"SMTP Connection Error: {e}")

    success_count = 0
    fail_count = 0

    # 3. Process and Send Emails
    for index, row in df.iterrows():
        try:
            recipient_email = row['Email']
            row_data = row.to_dict()

            msg = MIMEMultipart()
            msg['From'] = smtp_email
            msg['To'] = recipient_email
            msg['Subject'] = subject.format(**row_data)
            msg['Message-ID'] = f"<{uuid.uuid4()}@{socket.gethostname()}>"
            msg['Date'] = formatdate(localtime=True)

            body = DEFAULT_BODY.format(**row_data)
            msg.attach(MIMEText(body, 'plain'))

            server.send_message(msg)
            success_count += 1
        except Exception as e:
            fail_count += 1

    server.quit()

    return {
        "status": "completed",
        "emails_sent": success_count,
        "emails_failed": fail_count
    }