from minio import Minio
from minio.error import S3Error
import os
from minio.commonconfig import CopySource

MINIO_ENDPOINT = "localhost:9000"   
ACCESS_KEY = "minioadmin"
SECRET_KEY = "minioadmin"
USE_HTTPS = False

BUCKET_NAME = "demo-basic-ops"
OBJECT_NAME = "demo.txt"
OBJECT_COPY_NAME = "demo_copy.txt"
OBJECT_RENAMED_NAME = "demo_renamed.txt"
DOWNLOAD_PATH = "demo_downloaded.txt"


def get_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=ACCESS_KEY,
        secret_key=SECRET_KEY,
        secure=USE_HTTPS,
    )


def ensure_bucket(client, bucket_name):
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        print(f"[OK] Bucket créé : {bucket_name}")
    else:
        print(f"[INFO] Bucket déjà existant : {bucket_name}")


def create_local_file(path):
    with open(path, "w", encoding="utf-8") as f:
        f.write("Bonjour MinIO \n")
        f.write("Ceci est un fichier de test pour les opérations basiques.\n")
    print(f"[OK] Fichier local créé : {path}")


def upload_file(client, bucket, object_name, file_path):
    client.fput_object(bucket, object_name, file_path)
    print(f"[OK] Upload → s3://{bucket}/{object_name}")


def list_objects(client, bucket):
    print(f"[INFO] Contenu du bucket {bucket} :")
    for obj in client.list_objects(bucket, recursive=True):
        print(f" - {obj.object_name} ({obj.size} bytes)")


def copy_object(client, bucket, src_object, dst_object):
    source = CopySource(bucket,src_object)
    client.copy_object(bucket, dst_object, source)
    print(f"[OK] Copie {src_object} → {dst_object}")


def rename_object(client, bucket, src_object, dst_object):
    source = CopySource(bucket,src_object)
    client.copy_object(bucket, dst_object, source)
    client.remove_object(bucket, src_object)
    print(f"[OK] Rename {src_object} → {dst_object}")


def download_file(client, bucket, object_name, download_path):
    client.fget_object(bucket, object_name, download_path)
    print(f"[OK] Download → {download_path}")


def delete_object(client, bucket, object_name):
    client.remove_object(bucket, object_name)
    print(f"[OK] Suppression s3://{bucket}/{object_name}")


def main():
    client = get_client()


    ensure_bucket(client, BUCKET_NAME)


    create_local_file(OBJECT_NAME)

  
    upload_file(client, BUCKET_NAME, OBJECT_NAME, OBJECT_NAME)

    list_objects(client, BUCKET_NAME)


    copy_object(client, BUCKET_NAME, OBJECT_NAME, OBJECT_COPY_NAME)
    list_objects(client, BUCKET_NAME)


    rename_object(client, BUCKET_NAME, OBJECT_COPY_NAME, OBJECT_RENAMED_NAME)
    list_objects(client, BUCKET_NAME)


    download_file(client, BUCKET_NAME, OBJECT_RENAMED_NAME, DOWNLOAD_PATH)

  
    delete_object(client, BUCKET_NAME, OBJECT_NAME)
    delete_object(client, BUCKET_NAME, OBJECT_RENAMED_NAME)
    list_objects(client, BUCKET_NAME)


    # for path in (OBJECT_NAME, DOWNLOAD_PATH):
    #     if os.path.exists(path):
    #         os.remove(path)
    #         print(f"[OK] Fichier local supprimé : {path}")

    # print("[FIN] Démo des opérations basiques terminée ✅")


if __name__ == "__main__":
    try:
        main()
    except S3Error as e:
        print("Erreur MinIO:", e)
    except Exception as e:
        print("Erreur:", e)
