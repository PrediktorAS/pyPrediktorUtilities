import pytest
import smtplib
import logging
from pydantic import ValidationError

from pyprediktorutilities.send_email import SendEmail

srv = "smtp.gmail.com"
port = 587
usr = "username"
pwd = "password"
sndr = "nobody@somedomain.com"
rcpts = [sndr]
subj = "Test subject"
body = "Test body"

def test_send_email_wrong_port():
    with pytest.raises(ValidationError):
        SendEmail(srv, "not_valid_port", usr, pwd)

def test_send_email_missing_port():
    with pytest.raises(ValidationError):
        SendEmail(srv, None, usr, pwd)

def test_send_email_missing_server():
    with pytest.raises(ValidationError):
        SendEmail(None, port, usr, pwd)

def test_send_email_missing_username():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, None, pwd)

def test_send_email_missing_password():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, usr, None)

def test_send_email_missing_from_email():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, usr, pwd).send_email(None, rcpts, subj, body)
    
def test_send_email_missing_recipients():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, usr, pwd).send_email(sndr, None, subj, body)
        
def test_send_email_empty_recipients():
    with pytest.raises(ValueError):
        SendEmail(srv, port, usr, pwd).send_email(sndr, [], subj, body)

def test_send_email_missing_subject():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, usr, pwd).send_email(sndr, rcpts, None, body)

def test_send_email_missing_body():
    with pytest.raises(ValidationError):
        SendEmail(srv, port, usr, pwd).send_email(sndr, rcpts, subj, None)

def test_send_email_no_such_files():
    with pytest.raises(FileNotFoundError):
        SendEmail(srv, port, usr, pwd).send_email(sndr, rcpts, subj, body, ["/no/such/file.txt"])

def test_mocked_send_email_forced_fail(mocker):
    # mock the smtplib.SMTP object
    mock_SMTP = mocker.MagicMock(name="pyprediktorutilities.send_email.smtplib.SMTP")
    mocker.patch("pyprediktorutilities.send_email.smtplib.SMTP", new=mock_SMTP)
    mock_SMTP.side_effect = smtplib.SMTPException
    with pytest.raises(smtplib.SMTPException):
        SendEmail(srv, port, usr, pwd).send_email(sndr, rcpts, subj, body)
