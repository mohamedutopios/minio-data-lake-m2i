from minio import Minio
from minio.error import S3Error
import csv
import os
import string

MINIO_ENDPOINT = "localhost:9000"   
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
USE_HTTPS = False


BUCKET_AM = "customers-am-bucket"
BUCKET_NZ = "customers-nz-bucket"


FILE_AM = "customers_A_M.txt"
FILE_NZ = "customers_N_Z.txt"


INPUT_CSV = "customers.csv"


def ensure_bucket(client: Minio, bucket_name: str) -> None:
    """
    Vérifie si un bucket existe, sinon le crée.
    """
    if client.bucket_exists(bucket_name):
        print(f"[INFO] Le bucket '{bucket_name}' existe déjà, ok.")
    else:
        client.make_bucket(bucket_name)
        print(f"[OK] Bucket créé : {bucket_name}")


def split_csv(input_path: str, file_am: str, file_nz: str) -> None:
    """
    Lit le CSV source et sépare les lignes en deux fichiers :
      - A-M  -> file_am (TXT, tabulé)
      - N-Z  -> file_nz (CSV)
    en fonction de la première lettre du champ 'Country'.
    """

    letters_am = set(string.ascii_uppercase[string.ascii_uppercase.index("A"):string.ascii_uppercase.index("M")+1])
    letters_nz = set(string.ascii_uppercase[string.ascii_uppercase.index("N"):])

    with open(input_path, mode="r", encoding="utf-8") as f_in, \
         open(file_am, mode="w", encoding="utf-8") as f_am, \
         open(file_nz, mode="w", newline="", encoding="utf-8") as f_nz:

        reader = csv.DictReader(f_in)

      
        fieldnames = reader.fieldnames
        if fieldnames is None:
            raise ValueError("Le fichier CSV source est vide ou sans en-tête.")

        writer_nz = csv.DictWriter(f_nz, fieldnames=fieldnames)
        writer_nz.writeheader()

       
        f_am.write("\t".join(fieldnames) + "\n")

        
        for row in reader:
            country = (row.get("Country") or "").strip()
            if not country:
                
                continue

            first_letter = country[0].upper()

            if first_letter in letters_am:
                
                line_values = [row.get(col, "") for col in fieldnames]
                f_am.write("\t".join(line_values) + "\n")
            elif first_letter in letters_nz:
               
                writer_nz.writerow(row)
            else:
              
             
                print(f"[WARN] Pays ignoré (lettre hors A-Z) : {country}")


def upload_files(client: Minio, bucket_am: str, bucket_nz: str,
                 file_am: str, file_nz: str) -> None:
    """
    Upload les fichiers générés dans les buckets correspondants.
    """
    if os.path.exists(file_am):
        client.fput_object(bucket_am, os.path.basename(file_am), file_am)
        print(f"[OK] {file_am} uploadé dans s3://{bucket_am}/{os.path.basename(file_am)}")
    else:
        print(f"[ERR] Fichier manquant : {file_am}")

    if os.path.exists(file_nz):
        client.fput_object(bucket_nz, os.path.basename(file_nz), file_nz)
        print(f"[OK] {file_nz} uploadé dans s3://{bucket_nz}/{os.path.basename(file_nz)}")
    else:
        print(f"[ERR] Fichier manquant : {file_nz}")


def main():

    client = Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=USE_HTTPS,
    )

  
    ensure_bucket(client, BUCKET_AM)
    ensure_bucket(client, BUCKET_NZ)

   
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(f"Le fichier source '{INPUT_CSV}' est introuvable dans le répertoire courant.")

    split_csv(INPUT_CSV, FILE_AM, FILE_NZ)
    print("[OK] Fichiers générés localement.")

   
    upload_files(client, BUCKET_AM, BUCKET_NZ, FILE_AM, FILE_NZ)

    print("[FINI] TP terminé avec succès.")


if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print("Erreur MinIO:", e)
    except Exception as e:
        print("Erreur:", e)
