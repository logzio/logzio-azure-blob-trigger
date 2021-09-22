import azure.functions as func
from .file_handler import FileHandler


def main(blobfile: func.InputStream) -> None:
    FileHandler(blobfile.name, blobfile, blobfile.length).handle_file()
