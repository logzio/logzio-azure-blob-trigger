import azure.functions as func

from .file_handler import FileHandler


def main(blobfile: func.InputStream) -> None:
    blob_data = blobfile.read().decode("utf-8")
    
    FileHandler(blobfile.name, blob_data).handle_file()
