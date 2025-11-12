docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -v "C:\Users\Administrateur\Documents\data-lake\demo\minio-data":/data \
  quay.io/minio/minio server /data --console-address ":9001"

  docker run -d `
  --name minio `
  -p 9000:9000 `
  -p 9001:9001 `
  -e "MINIO_ROOT_USER=minioadmin" `
  -e "MINIO_ROOT_PASSWORD=minioadmin" `
  -v "C:\Users\Administrateur\Documents\data-lake\demo\minio-data:/data" `
  quay.io/minio/minio server /data --console-address ":9001"