﻿Документация
--------------------
ETL-процесс в формате SCD1

Техническое задание
------------------------------
Разработать ETL процесс, получающий ежедневную выгрузку данных (предоставляется за 3 дня), загружающий ее в хранилище данных и ежедневно строящий отчет. 
Выгрузка данных. 
Ежедневно некие информационные системы выгружают три следующих файла: 
1. Список транзакций за текущий день. Формат – CSV. 
2. Список терминалов полным срезом. Формат – XLSX. 
3. Список паспортов, включенных в «черный список» - с накоплением с начала месяца. Формат – XLSX. 
Сведения о картах, счетах и клиентах хранятся в СУБД PostgreSQL. Реквизиты для подключения: 
∙ Host: 
∙ Port: 
∙ Database: 
∙ User: 
∙ Password: 
Вам предоставляется выгрузка за последние три дня, ее надо обработать. 
Структура хранилища. 
Данные должны быть загружены в хранилище со следующей структурой (имена сущностей указаны по существу, без особенностей правил нейминга, указанных далее). 
Типы данных в полях можно изменять на однородные если для этого есть необходимость. Имена полей менять нельзя. Ко всем таблицам SCD1 должны быть добавлены технические поля create_dt, update_dt.

Описание проекта
--------------------------
1. DDL - создание таблиц-хранилищ (стейджинговый слой и целевой слой с делением на таблицы-факты и таблицы-измерения)
2. DML - Загрузка данных в стейджинговый слой "как есть". 
3. DML - Перенос обновленных, новых или помеченных на удаление данных в целевые хранилища - таблицы-факты ("как есть"), таблицы-измерения (только измененные данные с меткой формата SCD1)
4. DML - На целевых хранилищак построена витрина данных для аналитики.
5. DML - Сделан аналитический блок по поиску мошеннических транзакций и выгрузки данных клиентов, которые попали в статус подозрительных.
6. Выполнен crontab файл.

Описание данных
-------------------------
Из одного источника *.csv файлы
Из другого источника *.xlsx файлы

Технологии
-----------------
Pandas
SQL (PostgreSQL)
