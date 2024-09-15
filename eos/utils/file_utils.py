import dataclasses
import os
import re
import tempfile
import zipfile
from pathlib import Path


@dataclasses.dataclass
class FileData:
    filename: str
    content: bytes


@dataclasses.dataclass
class FolderData:
    folder_name: str
    content: bytes


def read_file(filename: str) -> FileData:
    """
    Reads a file and returns its content along with its name in a FileData object.

    :param filename: The name of the file to be read.
    :return: FileData object containing the filename and binary buffer.
    """
    with Path(filename).open("rb") as file:
        buffer = file.read()
    return FileData(Path(filename).name, buffer)


def write_file(file_data: FileData) -> None:
    """
    Writes the binary buffer from a FileData object to a file.

    :param file_data: The FileData object containing the filename and binary buffer.
    """
    with Path(file_data.filename).open("wb") as file:
        file.write(file_data.content)


def find_files_with_pattern(directory: str, pattern: str) -> list:
    """
    Search for files in the specified directory that match the given regex pattern.

    :param directory: The directory path to search in.
    :param pattern: The regex pattern to match filenames against.
    :return: A list of filenames that match the pattern.
    """
    matched_files = []
    regex = re.compile(pattern)

    if not Path(directory).is_dir():
        raise FileNotFoundError(f"Directory '{directory}' does not exist.")

    for _, _, filenames in os.walk(directory):
        for filename in filenames:
            if regex.match(filename):
                matched_files.append(filename)

    return matched_files


def find_highest_numbered_files(file_list: list) -> list:
    """
    Find all files with the greatest number following the dash or underscore in their names.
    All files returned will have the same number.
    Works with files formatted as "*-NUM.*"

    :param file_list: List of filenames.
    :return: List of filenames with the greatest common number.
    """
    number_pattern = re.compile(r"[\-_]([0-9]+)\.")

    numbers = {}
    for file in file_list:
        match = number_pattern.search(file)
        if match:
            number = int(match.group(1))
            if number in numbers:
                numbers[number].append(file)
            else:
                numbers[number] = [file]

    if not numbers:
        return []

    max_number = max(numbers.keys())

    return numbers[max_number]


def read_folder(folder_name: str) -> FolderData:
    """
    Zips a folder and reads it as a binary buffer.

    :param folder_name: The name of the folder to be zipped and read.
    :return: FolderData object containing the folder name and binary buffer of the zipped folder.
    """
    with tempfile.TemporaryFile() as temp_zip:
        with zipfile.ZipFile(temp_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
            for root, _, files in os.walk(folder_name):
                for file in files:
                    zipf.write(
                        Path(root) / file,
                        os.path.relpath(Path(root) / file, Path(folder_name).parent),
                    )

        temp_zip.seek(0)
        buffer = temp_zip.read()

    return FolderData(Path(folder_name).name, buffer)


def write_folder(folder_data: FolderData) -> None:
    """
    Decompresses the zip binary buffer and recreates the folder with a new name.

    :param folder_data: The FolderData object containing the new folder name and binary buffer.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
        zip_filename = Path(temp_dir) / "temp.zip"

        with Path(zip_filename).open("wb") as file:
            file.write(folder_data.content)

        with zipfile.ZipFile(zip_filename, "r") as zip_ref:
            zip_ref.extractall(temp_dir)

        root_folder = next(os.walk(temp_dir))[1][0]
        extracted_folder_path = Path(temp_dir) / root_folder

        Path(extracted_folder_path).rename(folder_data.folder_name)
