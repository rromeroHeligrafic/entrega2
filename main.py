import os
import urllib.request
import zipfile
import requests
import pandas as pd
import csv

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
            
            
    def txt_to_csv(self, input_folder):
        """
        Converts all text files in the given input folder to CSV format and saves them in the same directory.
        
        Parameters:
            input_folder (str): The path to the folder containing the text files.
        
        Returns:
            None
        
        Raises:
            Exception: If an error occurs during the conversion process.
        """
        for root, dirs, files in os.walk(input_folder):
            for file in files:
                if file.endswith(".txt"):
                    txt_file_path = os.path.join(root, file)
                    csv_file_path = os.path.splitext(txt_file_path)[0] + ".csv"
                    
                    try:
                        # Leer el archivo txt en un DataFrame
                        df = pd.read_csv(txt_file_path, delimiter=",")  # Ajusta el delimitador según sea necesario
                        
                        # Guardar el DataFrame en un archivo CSV
                        df.to_csv(csv_file_path, index=False, quoting=csv.QUOTE_NONE, escapechar='\\')
                        print(f"Convertido: {txt_file_path} a {csv_file_path}")
                        
                        # Eliminar el archivo txt después de convertirlo
                        os.remove(txt_file_path)
                        print(f"Eliminado: {txt_file_path}")
                    except Exception as e:
                        print(f"Error al convertir {txt_file_path}: {e}")

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
        input_folder = os.path.dirname(os.path.abspath(__file__))
        
        self.txt_to_csv(input_folder)

if __name__ == "__main__":
    url = "https://us-central1-duoc-bigdata-sc-2023-01-01.cloudfunctions.net/datos_transporte_et"
    file_manager = FileManager()
    file_manager.procesar_url(url)
