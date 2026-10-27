# Запуск ETL-конвеєра в офіційному Docker-образі Apache Spark (Java 17, Python 3, Spark 4.0.1).
# Використання:  .\run_spark.ps1                  - усі завдання
#                .\run_spark.ps1 -Tasks 1,7        - окремі завдання
#                .\run_spark.ps1 -Explore          - аналіз наборів даних
param([string]$Tasks = 'all', [switch]$Explore)

$mainArgs = @(if ($Explore) { '--explore' } else { '--tasks'; $Tasks })
$project = $PSScriptRoot
docker run --rm `
    -v "${project}:/app" `
    -w /app `
    -e PYTHONPATH=/app `
    -e PYTHONIOENCODING=utf-8 `
    spark:4.0.1-python3 `
    /opt/spark/bin/spark-submit --driver-memory 6g --conf spark.log.level=WARN main.py @mainArgs
