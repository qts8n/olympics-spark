# olympics-spark

>Spark-приложение для потоковой обработки данных олимпийских достижений и рекордов

## Запуск

Все настройки приложения находятся в файле `config.properties` в ресурсной директории проетка.

Для успешного запуска приложение требует подключение к очереди Google Pub/Sub.
Для локальной разработки рекомендуется использовать эмулятор данной очереди.
Эмулятор устанавливается отдельным модулем к утилите `gcloud` и запускается
как веб-сервер на 8085 порту по умолчанию. Команды для запуска сервера
и остальное описание CLI можно найти [тут](https://cloud.google.com/pubsub/docs/emulator#installing_the_emulator).

Для обработки сообщений из очереди ее нужно наполнить данными. Для этого используется датасет
[отсюда](https://www.kaggle.com/heesoo37/120-years-of-olympic-history-athletes-and-results).
CSV-файл `athlete_events.csv` достаточно положить в ресурсную директорию.

### Запуск эмулятора

Рекомендуемым способом работы с эмулятором является официальный образ docker-контейнера
[google/cloud-sdk](https://hub.docker.com/r/google/cloud-sdk/).

#### Запуск эмулятора в docker-контейнере

Эмулятор запускается в интерактивном режиме (`-it`) и показывает свой access-лог.
После отправки прерывания или завершения процесса иным способом контейнер удалится.

```bash
docker run --rm -it --volumes-from gcloud-config --network host --name pub-sub \
    google/cloud-sdk gcloud beta emulators pubsub start --project=olympics-269511
```

Важно учесть, что здесь и далее при запуске контейнеров образа google/cloud-sdk
рекомендуется указывать ключ `--volumes-from gcloud-config`, где `gcloud-config` -
контейнер с данными об авторизации в сервисах Google.

Если приведенная выше команда не работает, попробуйте запустить образ так (на Windows
также придется удалить символы `\` в конце строк и объединить их в одну):

```bash
docker run --rm -it --volumes-from gcloud-config -p "8085:8085" --name pub-sub \
    google/cloud-sdk gcloud beta emulators pubsub start --project=olympics-269511 \
    --host-port=0.0.0.0:8085
```

#### Создание контейнера с данными об авторизации

```bash
docker run -it --name gcloud-config google/cloud-sdk gcloud auth login
```

#### Альтернативный запуск эмулятора вне контейнера

Для запуска эмулятора вне контейнера нужно установить его модуль для утилиты `gcloud`:

```bash
gcloud components install pubsub-emulator
gcloud components update
```

После этого будут доступны команды `beta emulators`. Для запуска установленного
эмулятора введите следующую команду:

```bash
gcloud beta emulators pubsub start --project=olympics-269511
```

### Работа с очередью без эмулятора

Рассматривается также вариант работы с Pub/Sub напрямую без эмулятора. Для этого вы должны быть
членом проекта Google Cloud и иметь JSON-файл, содержащий приватный ключ для аутентификации
сервиса. Этот файл можно найти на странице проекта: APIs & Services > Credentials.

После скачивания JSON-файла из раздела Service Accounts этой же страницы его нужно поместить
в любую удобную вам директорию и указать на его путь переменную среды `GOOGLE_APPLICATION_CREDENTIALS`.
Подробнее об этом можно прочесть [тут](https://cloud.google.com/docs/authentication/getting-started#setting_the_environment_variable).

### Зависимости

Для управления зависимостями используется Maven. После `mvn install` вам должны быть доступны
все необходимые для запуска и разработки пакеты.

### Запуск Spark-job на кластерах Dataproc

#### Подготовка проекта

Чтобы запустить приложение на Dataproc нужно выполнить ряд действий в Cloud Shell
для подготовки окружения.

Включение необходимых сервисов:

```bash
gcloud services enable \
  dataproc.googleapis.com \
  pubsub.googleapis.com 
```

Создания топика и подписка в очереди Pub/Sub:

```bash
export TOPIC=olympics-topic
export SUBSCRIPTION=olympics-sub

gcloud pubsub topics create $TOPIC
gcloud pubsub subscriptions create $SUBSCRIPTION --topic=$TOPIC
```

Создание базы данных в BigQuery:

```bash
export DATASET=dataset

bq --location=europe-west3 mk \
  --dataset \
  $DATASET
```

Создание сервисного аккаунта и добавление необходимых привилегий:

```bash
export PROJECT=$(gcloud info --format='value(config.project)')
export SERVICE_ACCOUNT_NAME=dataproc-service-account
export SERVICE_ACCOUNT_ADDRESS=$SERVICE_ACCOUNT_NAME@$PROJECT.iam.gserviceaccount.com

gcloud iam service-accounts create $SERVICE_ACCOUNT_NAME

gcloud projects add-iam-policy-binding $PROJECT \
  --role roles/dataproc.worker \
  --member="serviceAccount:$SERVICE_ACCOUNT_ADDRESS"

gcloud beta pubsub subscriptions add-iam-policy-binding \
  $SUBSCRIPTION \
  --role roles/pubsub.subscriber \
  --member="serviceAccount:$SERVICE_ACCOUNT_ADDRESS"

gcloud projects add-iam-policy-binding $PROJECT \
  --role roles/bigquery.dataEditor \
  --member="serviceAccount:$SERVICE_ACCOUNT_ADDRESS"

gcloud projects add-iam-policy-binding $PROJECT \
  --role roles/bigquery.jobUser \
  --member="serviceAccount:$SERVICE_ACCOUNT_ADDRESS"
```

Запуск Dataproc кластеров:

```bash

export CLUSTER=demo-cluster

gcloud dataproc clusters create demo-cluster \
  --region=europe-west3 \
  --zone=europe-west3-a \
  --master-machine-type=n1-standard-4 \
  --num-workers=2 \
  --worker-machine-type=n1-standard-2 \
  --scopes=pubsub,bigquery \
  --image-version=1.2 \
  --service-account=$SERVICE_ACCOUNT_ADDRESS
```

После успешного выполнения предыдущих действий вам нужно подготовить
хранилище для датасета и исполняемых jar-файлов. Для этого:

1. создайте bucket (`gsutil mb -l europe-west3 gs://my-very-own-bucket-1`)
2. загрузите туда датасет в формате csv
3. загрузите также необходимые jar-файлы

Используемый коннектор для BigQuery требует временного хранилища, которое должно
находится в том же регионе, что и хранилище с jar-файлом. Про компиляцию
в jar-файл подробнее смотрите [далее](#run-spark-job).

```bash
export TMP_BUCKET=some-bucket-543645434

gsutil mb -l europe-west3 gs://$TMP_BUCKET
``` 

#### Генератор сообщений для Pub/Sub

Spark-job получает сообщения из Pub/Sub подписки, которая была создана на этапе
подготовки проекта. На момент создания подписки она пуста. Чтобы загрузить в очередь
датасет используйте генератор. Для этого:

1. загрузите исходный код генератора из папки `generator` в корне проекта на VM instance
2. создайте виртуальное окружение для python *(may take some time)*
3. установите требуемые зависимости из файла `requirements.txt`
4. запустите генератор

```bash
export PROJECT=$(gcloud info --format='value(config.project)')
export GENERATOR_PATH=~/generator
export TOPIC=olympics-topic
export BUCKET=my-very-own-bucket-1
export DIRECTORY=data

cd $GENERATOR_PATH
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

python generatord.py $PROJECT $TOPIC $BUCKET $DIRECTORY 5 50000 &

deactivate
```

**NOTE:** Здесь переменные `$BUCKET` и `$GENERATOR_PATH` могут разниться с тем,
что есть у вас.

#### Run Spark-job

Для того, чтобы скомпилировать jar-файл для запуска клонируйте
этот репозиторий и соберите package при помощи Maven:

1. задайте нужные настройки в файле `config.properties` в папке `resources`
2. установите зависимости (`mvn install`)
3. соберите package (`mvn clean package -DskipTests`)
4. подготовьте package (`zip -d target/olympics-spark-1.0-SNAPSHOT.jar META-INF/*.RSA META-INF/*.DSA META-INF/*.SF`)

Исполняемый файл готов и находится в папке `target`. Перенесите его в свой bucket.
Еще раз убедитесь, что проект Google Cloud собран правильно, а именно:

1. в подписке Pub/Sub есть сообщения
2. jar-файл находится в bucket
3. датасет в BigQuery создан
4. временный bucket создан и находится в том же регионе

Запустите Spark-job через Cloud Sell:

```bash
export PROJECT=$(gcloud info --format='value(config.project)')
export CLUSTER=demo-cluster
export BUCKET=my-very-own-bucket-1
export JAR_NAME=olympics-spark-1.0-SNAPSHOT.jar
export JAR="gs://$BUCKET/jars/$JAR_NAME"
export SPARK_PROPERTIES="spark.dynamicAllocation.enabled=false,spark.streaming.receiver.writeAheadLog.enabled=true"
export ARGUMENTS="10 10 5 hdfs:///user/checkpoint"

gcloud dataproc jobs submit spark \
  --region=europe-west3 \
  --cluster $CLUSTER \
  --async \
  --jar $JAR \
  --max-failures-per-hour 10 \
  --properties $SPARK_PROPERTIES \
  -- $ARGUMENTS
```

Подробнее о работе с GCP [тут](https://cloud.google.com/solutions/using-apache-spark-dstreams-with-dataproc-and-pubsub#submitting_the_spark_streaming_job)
и [тут](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark).
