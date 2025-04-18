from pyicloud import PyiCloudService
import getpass
import json
from shutil import copyfileobj
import os

class ICloudFolderLister:
    def __init__(self, icloud_username, icloud_password):
        """
        Initialize the class and authenticate to iCloud.

        :param icloud_username: iCloud username (email).
        :param icloud_password: iCloud password.
        """
        self.icloud = self._authenticate_icloud(icloud_username, icloud_password)

    def _authenticate_icloud(self, username, password):
        icloud = PyiCloudService(username, password)
        if icloud.requires_2fa:
            print("Two-factor authentication required. Please enter the code:")
            code = input("Enter the 2FA code: ")
            if not icloud.validate_2fa_code(code):
                raise Exception("Invalid 2FA code.")
        return icloud

    def list_root_folders(self):
        """
        List all files present in the specified folders and return their content as a JSON object.

        :return: JSON object with file names as keys and their content as values.
        """
        folders_to_scan = [
            self.icloud.drive['Obsidian']['WizeCosm']['00 - Univers']['Politique'],
            self.icloud.drive['Obsidian']['WizeCosm']['01 - Géographie']['Continents'],
            self.icloud.drive['Obsidian']['WizeCosm']['00 - Univers']['Les Races']
        ]

        files_content = {}

        for folder in folders_to_scan:
            print(f"Scanning folder: {folder.name}")
            for file_name in folder.dir():
                drive_file = folder[file_name]
                if drive_file.type == 'file':  # Ensure it's a file
                    print(f"Processing file: {drive_file.name}")
                    try:
                        # Read the file content
                        with drive_file.open(stream=True) as response:
                            file_content = response.raw.read().decode('utf-8')  # Decode content as text
                            files_content[drive_file.name] = file_content
                    except Exception as e:
                        print(f"Error reading file {drive_file.name}: {e}")

        # Return the content as a JSON object
        return json.dumps(files_content, indent=4)

    def export_to_json(self, json_data, output_path):
        """
        Export the JSON data to a file.

        :param json_data: JSON data to export.
        :param output_path: Path to save the JSON file.
        """
        try:
            with open(output_path, 'w', encoding='utf-8') as json_file:
                json_file.write(json_data)
            print(f"JSON data successfully exported to {output_path}")
        except Exception as e:
            print(f"Error exporting JSON data: {e}")


if __name__ == "__main__":
    # Prompt for iCloud credentials
    icloud_username = input("Enter your iCloud email: ").strip()
    icloud_password = getpass.getpass("Enter your iCloud password: ").strip()

    try:
        # Initialize the class
        print("\nAuthenticating to iCloud...")
        folder_lister = ICloudFolderLister(icloud_username, icloud_password)

        # List root folders and get file content
        print("\nFetching file content from specified folders...")
        files_json = folder_lister.list_root_folders()

        # Export JSON to a file
        script_dir = os.path.dirname(os.path.abspath(__file__))  # Get the script's directory
        output_file = os.path.join(script_dir, "icloud_files_content.json")
        folder_lister.export_to_json(files_json, output_file)

    except Exception as e:
        print(f"\nAn error occurred: {e}")