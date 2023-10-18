import pytest
import logging
from pydantic import ValidationError

from pyprediktorutilities.file_transfer import SFTPClient

server = "someserver.somedomain.com"
username = "username"
password = "password"
port = 22

def test_instance_of_sftp_client():
    sftp_client = SFTPClient(server=server, username=username, password=password, port=port)
    assert isinstance(sftp_client, SFTPClient)
    assert sftp_client.server == server
    assert sftp_client.username == username
    assert sftp_client.password == password
    assert sftp_client.port == port

def test_sftp_client_with_invalid_server():
    with pytest.raises(ValidationError):
        SFTPClient(server=None, username=username, password=password, port=port)

def test_sftp_client_with_invalid_port():
    with pytest.raises(ValidationError):
        SFTPClient(server=server, username=username, password=password, port="No_port")

def test_sftp_client_with_invalid_username():
    with pytest.raises(ValidationError):
        SFTPClient(server=server, username=123, password=password, port=port)

def test_sftp_client_with_invalid_password():
    with pytest.raises(ValidationError):
        SFTPClient(server=server, username=username, password=None, port=port)

def test_sftp_client_with_missing_file():
    with pytest.raises(FileNotFoundError):
        SFTPClient(server=server, username=username, password=password, port=port).upload(files=[], remote_directory="/tmp")

def test_sftp_client_with_nonexisting_file():
    with pytest.raises(FileNotFoundError):
        SFTPClient(server=server, username=username, password=password, port=port).upload(files=["No_such_file"], remote_directory="/tmp")