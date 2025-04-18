from pyicloud import PyiCloudService
import getpass

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
        List all folders present at the root of iCloud Drive.

        :return: List of folder names.
        """
        root = self.icloud.drive['Obsidian']['WizeCosm'].dir()  # Use the dir() method to list root contents
        folders = []
        print("Root folder contents:")
        for folder_name in root:
            print(f"Item: {folder_name}")  # Debugging output
            folders.append(folder_name)
        return folders


if __name__ == "__main__":
    # Prompt for iCloud credentials
    icloud_username = input("Enter your iCloud email: ").strip()
    icloud_password = getpass.getpass("Enter your iCloud password: ").strip()

    try:
        # Initialize the class
        print("\nAuthenticating to iCloud...")
        folder_lister = ICloudFolderLister(icloud_username, icloud_password)

        # List root folders
        print("\nListing all folders at the root of iCloud Drive:")
        folders = folder_lister.list_root_folders()
        for idx, folder in enumerate(folders, 1):
            print(f"{idx}. {folder}")

    except Exception as e:
        print(f"\nAn error occurred: {e}")