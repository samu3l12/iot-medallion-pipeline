import json
import os

def validar_crudos():
    # Voy a leer el fichero recien generado para comprobar su formato basico
    file_path = "/tmp/raw_iot_data.jsonl"
    
    if not os.path.exists(file_path):
        print(f"No he encontrado el fichero {file_path}")
        return

    total = 0
    errores_json = 0
    
    with open(file_path, "r", encoding="utf-8") as f:
        # Leemos el archivo temporal de principio a fin, linea a linea
        for linea in f:
            total += 1
            try:
                # El metodo json.loads() intenta convertir el texto crudo a un Diccionario Python.
                # Si a la linea le falta una coma, un corchete o las comillas estan mal puestas,
                # Python no podra transformarlo y lanzara un "JSONDecodeError".
                json.loads(linea)
            except json.JSONDecodeError:
                # Si ocurre el error de sintaxis, capturamos el fallo (except) 
                # y sumamos 1 al contador de lineas corruptas en vez de detener el programa.
                errores_json += 1
                
    print(f"He revisado el formato inicial de {total} lineas. Lineas mal formadas: {errores_json}.")

if __name__ == "__main__":
    validar_crudos()
