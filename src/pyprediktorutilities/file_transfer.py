from pydantic import validate_call, AnyUrl
import os
import logging
import paramiko
from pyprediktorutilities.shared import validate_file

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class SFTPClient:
    """An SFTP client to upload files to a remote server
    
    Args:
        server (AnyUrl): The server address
        username (str): The username
        password (str): The password
        port (int): The port number
        
    Returns:
        Object: SFTPClient object
    """
    @validate_call
    def __init__(self, server: AnyUrl, username: str, password: str, port: int) -> object:
        self.server = server
        self.username = username
        self.password = password
        self.port = port

    @validate_call
    def upload(self, files: list[str], remote_directory: str):
        """Upload the files to the remote directory, create the directory
        recursively if it does not exist (and the remote server allows it)

        Args:
            files (list[str]): _description_
            remote_directory (str): _description_
        """
        
        for file in files:
            validate_file(file)
        
        with paramiko.SSHClient() as ssh:
            ssh.load_system_host_keys()
            ssh.connect(
                self.server,
                port=self.port,
                username=self.username,
                password=self.password,
            )

            with ssh.open_sftp() as sftp:
                # Check if remote directory exists or create it
                try:
                    sftp.stat(remote_directory)
                except FileNotFoundError:
                    sftp.mkdir(remote_directory)

                # Upload each file
                for file in files:
                    remote_path = f"{remote_directory}/{os.path.basename(file)}"
                    sftp.put(file, remote_path)
                    logging.info(
                        f"Uploaded {file} to {remote_path} successfully!"
                    )

