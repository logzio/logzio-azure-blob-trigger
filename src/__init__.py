import azure.functions as func

from .file_handler import FileHandler


def main(blobfile: func.InputStream) -> None:
    FileHandler(blobfile).handle_file()
    