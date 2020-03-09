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
После отправки прерывания или завершения процесса иным способом контейнер удалиться.

```bash
docker run --rm -it --volumes-from gcloud-config --network host --name pub-sub \
    google/cloud-sdk gcloud beta emulators pubsub start --project=olympics-269511
```

Важно учесть, что здесь и далее при запуске контейнеров образа google/cloud-sdk
рекомендуется указывать ключ `--volumes-from gcloud-config`, где `gcloud-config` -
контейнер с данными об авторизации в сервисах Google.

#### Создание контейнера с данными об авторизации

```bash
docker run -ti --name gcloud-config google/cloud-sdk gcloud auth login
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

Для запуска доступных Spark-задач необходимо собрать проект в JAR:

```bash
mvn clean package
```

Сам JAR-файл уже с помощью утилиты `gcloud` отправляется на уже запущенный кластер.
Подробнее об этом [тут](https://cloud.google.com/solutions/using-apache-spark-dstreams-with-dataproc-and-pubsub#submitting_the_spark_streaming_job)
и [тут](https://cloud.google.com/sdk/gcloud/reference/dataproc/jobs/submit/spark).
