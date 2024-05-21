import os
import urllib.request
import zipfile
import requests

class FileManager:
    def __init__(self):
        """
        Initializes a new instance of the class.

        This method initializes the `data` attribute of the class to an empty dictionary.

        Parameters:
            None

        Returns:
            None
        """
        self.data = {}

    #metodo para extraer la url
    def obtener_urls(self):
        """
        Retrieves a list of tuples containing the ID and URL of each resource in the 'result' section of the 'data' dictionary.
        Returns:
            List[Tuple[str, str]]: A list of tuples containing the ID and URL of each resource.
        """
        urls = []
        for resource in self.data['result']['resources']:
            urls.append((resource['id'], resource['url']))
        return urls

    #metodo para extraer 
    def obtener_urls_descarga(self, url_api):
        """
        Retrieves a list of URLs for downloading files from a given API endpoint.
        Args:
            url_api (str): The URL of the API endpoint.
        Returns:
            List[Tuple[str, str]]: A list of tuples containing the ID and URL of each file to be downloaded.
                                    Only files with the format "ZIP" are included in the list.
        Raises:
            Exception: If an error occurs during the retrieval process.
        Note:
            If the API endpoint does not return a successful response, an empty list is returned.
        """
        try:
            response = requests.get(url_api)
            data = response.json()
            if data["success"]:
                resources = data["result"]["resources"]
                urls_descarga = [(resource["id"], resource["url"]) for resource in resources if resource["format"] == "ZIP"]
                return urls_descarga
            else:
                print("No se pudo obtener el JSON de la URL.")
                return []
        except Exception as e:
            print("Ocurrió un error:", e)
            return []

    def descargar_archivos(self, archivos):
        """
        Downloads files from a list of tuples containing the ID and URL of each file.
        
        Args:
            archivos (List[Tuple[str, str]]): A list of tuples containing the ID and URL of each file to be downloaded.
        
        Yields:
            str: The name of the downloaded file.
        
        """
        for id, url in archivos:
            name_doc = f"{id}.zip"  # Utiliza el ID como nombre del archivo
            urllib.request.urlretrieve(url, name_doc)
            yield name_doc

    def descomprimir_archivos(self, archivos):
        """
        Extracts the contents of a list of zip files and removes the zip files.

        Args:
            archivos (List[str]): A list of paths to zip files.

        Returns:
            None
        """
        for archivo in archivos:
            folder = os.path.splitext(archivo)[0]
            with zipfile.ZipFile(archivo, 'r') as zip_ref:
                zip_ref.extractall(folder)
            os.remove(archivo)  # Elimina el archivo zip después de descomprimirlo

    def procesar_url(self, url_api):
        """
        Processes a given URL API by retrieving the download URLs of resources, storing them in the `data` attribute,
        downloading the files, and decompressing them.

        Parameters:
            url_api (str): The URL of the API to retrieve the download URLs from.

        Returns:
            None
        """
        urls_descarga = self.obtener_urls_descarga(url_api)
        self.data = {"result": {"resources": [{"id": id, "url": url} for id, url in urls_descarga]}}
        urls = self.obtener_urls()
        archivos_descargados = self.descargar_archivos(urls)
        self.descomprimir_archivos(archivos_descargados)

if __name__ == "__main__":
    url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    
    file_manager = FileManager()
    file_manager.procesar_url(url)
