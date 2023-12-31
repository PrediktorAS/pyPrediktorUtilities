from pydantic import validate_call, EmailStr
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import os
from pyprediktorutilities.shared import validate_file

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class SendEmail:
    """Helper function to send emails with attachments using SMTP
    
    Args:
        server (str): SMTP server address
        port (int): SMTP server port
        username (str): SMTP server username
        password (str): SMTP server password
        
    Returns:
        Object: SendEmail object
    """
    
    @validate_call
    def __init__(self, server: str, port: int, username: str, password: str) -> object:
        """Class initialiser

        Args:
            server (str): SMTP server address
            port (int): SMTP server port
            username (str): SMTP server username
            password (str): SMTP server password

        Returns:
            object: SendEmail object
        """
        self.smtp_server = server
        self.smtp_server_port = port
        self.smtp_server_username = username
        self.smtp_server_password = password

    @validate_call
    def send_email(self, from_email: EmailStr, recipients: list[EmailStr], subject: str, body: str, files: list = []):
        """_summary_

        Args:
            from_email (email address): The sender email address
            recipients (list of email addresses): list of one or more recipient email addresses
            subject (str): The email subject
            body (str): The email body
            files (list, optional): A list of paths to files to include. Defaults to [].
        """
        
        if len(recipients) == 0:
            logging.error("No recipients specified")
            raise ValueError("No recipients specified")
        
        msg = MIMEMultipart()
        msg["From"] = str(from_email)
        msg["Subject"] = subject
        msg["To"] = ', '.join(str(e) for e in recipients)
        msg.attach(MIMEText(body, "plain"))

        for file in files:
            validate_file(file)
            
            with open(file, "rb") as f:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(f.read())
                encoders.encode_base64(part)
                part.add_header(
                    "Content-Disposition",
                    f"attachment; filename={os.path.basename(file)}",
                )
                msg.attach(part)


        text = msg.as_string()
        reciplist = [str(e) for e in recipients]
        try:
            with smtplib.SMTP(self.smtp_server, self.smtp_server_port) as server:
                server.starttls()
                server.login(self.smtp_server_username, self.smtp_server_password)
                server.sendmail(str(from_email), reciplist, text)
                logging.info(f"Email sent to {reciplist} with subject {subject} and {len(files)} attachments")
        except smtplib.SMTPException as e:
            logging.error(f"Error: unable to send email to {reciplist} with subject {subject} and {len(files)} attachments")
            logging.error(e)
            raise e


if __name__ == "__main__":
    pass
