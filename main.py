#!/usr/bin/python3

'''Инструкция:
1) сначала почистить все таблицы блоком sql-кода из main.ddl
и загрузить оттуда же первое значение в meta
2) запустить скрипт main.py

реализация функциями, поэтому можно каждый день сделать отдельно:
first_load(filess[0]) - загрузка данных первого дня - идет "как есть"
etl_scd1(files) - в цикле обслуживает два следующих дня

хотелось бы знать, на этом уровне есть минимальный порог?
'''

import os
import pandas as pd
import psycopg2

# подключение к внешним схемам - edu или bank
from pandas import DataFrame


def connect(database, host, user, password, port):
    conn = psycopg2.connect(
        database=database,
        host=host,
        user=user,
        password=password,
        port=port)

    # отключение автокоммита
    conn.autocommit = False

    # возврат нужного подключения к схеме
    return conn


# разрыв соединения со схемами - edu или bank
def disconnect(conn, cursor):
    cursor.close()
    conn.close()
    print('Соединение закрыто')


# очистка стейджинговых таблиц
def clear_table(table, conn, cursor):
    cursor.execute(f'delete from de13ma.mrzn_{table}')
    conn.commit()


# формирование отчета на основе транзакций по просроченной карте
def report(conn: object, cursor: object, date: object) -> object:
    # дработка 30.04.23
    """ Просрочен / блок паспорта - 1
        Недейств. договор - 2
        Разные города - 3
        Подбор суммы - 4"""
    # очистка отчета за прошедший период
    clear_table('rep_fraud', conn, cursor)
    # подтверждение очистки отчета
    conn.commit()
    # формирование сводной таблицы для отлова мошеннических транзакций
    request = """WITH 
                 rep AS (
                        SELECT 
                            cl.client_id,
                            concat (cl.last_name,' ',cl.first_name,' ',cl.patronymic) as fio,
                            cl.passport_num,
                            cl.passport_valid_to,
                            cl.phone,
                            ac.valid_to acc_valid_to,
                            cr.card_num,
                            tr.trans_id,
                            tr.trans_date,
                            tr.amt,
                            tr.oper_result,
                            tr.terminal,
                            ter.terminal_city 
                        FROM de13ma.mrzn_dwh_dim_clients cl
                           LEFT JOIN de13ma.mrzn_dwh_dim_accounts ac ON cl.client_id = ac.client 
                           LEFT JOIN de13ma.mrzn_dwh_dim_cards cr ON ac.account_num = cr.account_num 
                           LEFT JOIN  de13ma.mrzn_dwh_fact_transactions tr ON substring(cr.card_num, 1, 19) = trim(tr.card_num)  
                           LEFT JOIN  de13ma.mrzn_dwh_dim_terminals ter on ter.terminal_id = tr.terminal
                        WHERE (cl.passport_num IN (SELECT passport_num FROM de13ma.mrzn_dwh_fact_passport_blacklist)
                            OR cl.passport_valid_to < tr.trans_date
                            OR ac.valid_to < tr.trans_date) AND tr.oper_type IN ('PAYMENT', 'WITHDRAW')
                                ),
                rep1 AS (
                        SELECT 
                            max(cl.client_id) client_id,
                            max(concat (cl.last_name,' ',cl.first_name,' ',cl.patronymic)) fio,
                            cl.passport_num,
                            cr.card_num card_num,
                            max(cl.phone) phone,
                            max(cr.card_num) card_num,
                            count(tr.trans_id) count_transactions,
                            max(tr.trans_date) trans_date,
                            lag(tr.amt, 1, 1000) over (partition by cr.card_num, cl.passport_num order by tr.trans_date) - tr.amt  amt,
                            min(tr.oper_result) oper_result,
                            lag(ter.terminal_city, 1, ter.terminal_city) over (partition by cr.card_num, cl.passport_num order by tr.trans_date) <> ter.terminal_city  city 
                        FROM de13ma.mrzn_dwh_dim_clients cl
                           LEFT JOIN de13ma.mrzn_dwh_dim_accounts ac ON cl.client_id = ac.client 
                           LEFT JOIN de13ma.mrzn_dwh_dim_cards cr ON ac.account_num = cr.account_num 
                           LEFT JOIN  de13ma.mrzn_dwh_fact_transactions tr ON substring(cr.card_num, 1, 19) = trim(tr.card_num)  
                           LEFT JOIN  de13ma.mrzn_dwh_dim_terminals ter on ter.terminal_id = tr.terminal
                        WHERE tr.oper_type IN ('PAYMENT', 'WITHDRAW')
                        GROUP BY tr.amt, ter.terminal_city, cr.card_num, cl.passport_num, tr.trans_date
                        ORDER BY fio, trans_date),  
                report AS (
                        SELECT 
                               trans_date event_dt,
                               passport_num passport, 
                               fio, 
                               phone, 
                               CASE 
                                WHEN trans_date > coalesce(acc_valid_to, to_timestamp('9999-12-31','YYYY-MM-DD'))
                                THEN 2
                                WHEN trans_date > coalesce(passport_valid_to, to_timestamp('9999-12-31','YYYY-MM-DD')) or passport_num IN (SELECT passport_num FROM de13ma.mrzn_dwh_fact_passport_blacklist)
                                THEN 1
                                ELSE 0
                               END event_type,
                               trans_date report_dt
                        FROM rep),
                report1 AS (
                        SELECT 
                                client_id,
                                fio,
                                passport_num,
                                phone,
                                CASE 
                                    WHEN not city
                                    THEN 3
                                    WHEN amt>0 and oper_result like '%REJECT%'
                                    THEN 4
                                    ELSE 0
                                   END event_type,
                                trans_date report_dt
                        FROM rep1)
                INSERT INTO de13ma.mrzn_rep_fraud(
                        event_dt,
                        passport,
                        fio,
                        phone,
                        event_type,
                        report_dt)
                SELECT 
                       max(event_dt),
                       passport,
                       max(fio),
                       max(phone),
                       event_type,
                       max(report_dt)
                FROM report
                WHERE event_type = 1 OR event_type = 2
                GROUP BY passport, event_type
                UNION
                SELECT 
                       max(event_dt),
                       passport,
                       max(fio),
                       max(phone),
                       event_type,
                       max(report_dt)
                FROM report
                WHERE event_type = 3 OR event_type = 4
                GROUP BY passport, event_type"""
    cursor.execute(request)
    # подтверждение внесения изменений в отчет
    conn.commit()
    request = """SELECT *
                FROM de13ma.mrzn_rep_fraud"""
    cursor.execute(request)
    # выгрузка отчета в csv-файл в архив с датой в имени
    try:
        rep = cursor.fetchall()
        names = [x[0] for x in cursor.description]
        rpt = pd.DataFrame(rep, columns=names)
        print(rpt)
        rpt.to_csv(f'archive/report_{date}.arch')
        print('Файл отчета выгружен')
    except:
        print('Нет пойманных транзакций')
    # подтверждение внесения изменений в отчет
    conn.commit()


# первая загрузка данных из разных источников на стейджинг "как есть"
def first_load(files):
    # создание подключения к схеме edu
    conn_edu = connect('edu', 'de-edu-db.chronosavant.ru',
                       'de13ma', 'meriadocbrandybuck', '5432')
    # создание курсора
    cursor_edu = conn_edu.cursor()

    # работа со стейджинговыми таблицами
    # очистка стэйджинговых таблиц
    tables = ['transactions', 'accounts', 'cards', 'clients', 'terminals', 'blacklist']
    for table in tables:
        clear_table(f'stg_{table}', conn_edu, cursor_edu)
    # 1. загрузка данных из файлов - список формируется каждый день и
    # подается на вход функции в виде названий трех файлов - трансакции, терминалы и черный список
    # данные идут для загрузки в стейджинговые таблицы без предобработки'''
    # загрузка в таблицу транзакций за прошедший день
    tr = pd.read_csv(f'data/{files[0]}', sep=';', decimal=',', header=0, index_col=None)
    tr['transaction_date'] = pd.to_datetime(tr['transaction_date'], format='%Y-%m-%d %H:%M:%S')
    # print(tr)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_transactions(
                            trans_id,
                            trans_date,
                            amt,
                            card_num,
                            oper_type,
                            oper_result,
                            terminal)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)""", tr.values.tolist())
    # обновление данных в таблице мета
    dt = max(tr['transaction_date'])
    cursor_edu.execute("""UPDATE de13ma.mrzn_stg_meta
                                SET max_update_dt = %s""", (dt,))
    # загрузка в таблицу терминалов за прошедший день
    term = pd.read_excel(f'data/{files[1]}', sheet_name='terminals', header=0, index_col=None)
    cursor_edu.executemany("""INSERT INTO mrzn_stg_terminals(
                            terminal_id,
                            terminal_type,
                            terminal_city,
                            terminal_address)
                            VALUES (%s, %s, %s, %s)""", term.values.tolist())
    # загрузка в таблицу черного списка за прошедший день
    blck = pd.read_excel(f'data/{files[2]}', sheet_name='blacklist', header=0, index_col=None)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_blacklist(
                            entry_dt,
                            passport_num)
                            VALUES (%s, %s)""", blck.values.tolist())
    # фиксация внесения данных в БД
    conn_edu.commit()

    # 2. загрузка стейджинговых таблиц из другого источника - обновляемой базы банка
    #  устанавливаем соединение с базой банка
    conn_bank = connect('bank', 'de-edu-db.chronosavant.ru',
                        'bank_etl', 'bank_etl_password', '5432')
    # создание курсора базы банк
    cursor_bank = conn_bank.cursor()

    # выгрузка данных из таблицы клиенты во временную переменную
    cursor_bank.execute("""SELECT client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone  
                            FROM info.clients""")
    clients = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    # выгрузка через пандас в стейджинговую таблицу клиентов из временной переменной
    clns = pd.DataFrame(clients, columns=names)
    cursor_edu.executemany(""" INSERT INTO de13ma.mrzn_stg_clients(  
                            client_id ,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone)
                            VALUES( %s, %s, %s, %s, %s, %s, %s, %s)""", clns.values.tolist())

    # выгрузка данных из базы банка по аккаунтам во временную переменную
    cursor_bank.execute("""SELECT account,
                            valid_to,
                            client 
                            FROM info.accounts""")
    accounts = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    # перегрузка данных из временной переменной в стейджинговую таблицу аккаунтов через пандас
    accnts = pd.DataFrame(accounts, columns=names)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_accounts(  
                            account_num,
                            valid_to,
                            client)
                            VALUES( %s, %s, %s)""", accnts.values.tolist())

    # загрузка через временную переменную данных по картам
    cursor_bank.execute("""SELECT card_num, 
                            account, 
                            create_dt, 
                            update_dt                            
                            FROM info.cards""")
    cards = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    crds = pd.DataFrame(cards, columns=names)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_cards(  
                            card_num, 
                            account_num, 
                            create_dt, 
                            update_dt)                             
                            VALUES( %s, %s, %s,%s )""", crds.values.tolist())
    # подтверждение внесения изменений в стейджинговые таблицы
    conn_edu.commit()
    # соединение с источником bank больше не нужно, разрываем его
    disconnect(conn_bank, cursor_bank)
    # выгрузка в целевые таблицы "как есть" с установкой create_dt и update_dt в таблицы-измерения
    # загрузка из стейджинга в dwh_dim_terminals
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_terminals(
                             terminal_id,
                             terminal_type,
                             terminal_city,
                             terminal_address,
                             create_dt,
                             update_dt)
                             SELECT 
                             stg.terminal_id,
                             stg.terminal_type,
                             stg.terminal_city,
                             stg.terminal_address,
                             to_timestamp('2021-03-01','YYYY-MM-DD'),
                             to_timestamp('9999-12-31','YYYY-MM-DD')
                             FROM  de13ma.mrzn_stg_terminals stg""")
    # загрузка из стейджинга в dwh_dim_cards
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_cards(
                            card_num,
                            account_num,
                            create_dt,
                            update_dt)
                            SELECT
                            cr.card_num,
                            cr.account_num,
                            cr.create_dt,
                            cr.update_dt
                            FROM de13ma.mrzn_stg_cards cr""")
    # загрузка из стейджинга в dwh_dim_accounts
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_accounts(
                            account_num,
                            valid_to,
                            client,
                            create_dt,
                            update_dt)
                            SELECT
                            ac.account_num,
                            ac.valid_to,
                            ac.client,
                            to_timestamp('2021-03-01','YYYY-MM-DD'),
                            to_timestamp('9999-12-31','YYYY-MM-DD')
                            FROM de13ma.mrzn_stg_accounts ac""")
    # загрузка из стеджинга в dwh_dim_clients
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_clients(
                            client_id,
                            last_name,
                            first_name,
                            patronymic,
                            date_of_birth,
                            passport_num,
                            passport_valid_to,
                            phone,create_dt,
                            update_dt)
                            SELECT
                            cl.client_id,
                            cl.last_name,
                            cl.first_name,
                            cl.patronymic,
                            cl.date_of_birth,
                            cl.passport_num,
                            cl.passport_valid_to,
                            cl.phone,
                            to_timestamp('2021-03-01','YYYY-MM-DD'),
                            to_timestamp('9999-12-31','YYYY-MM-DD')
                            FROM de13ma.mrzn_stg_clients cl""")

    # загрузка из стейджинга в dwh_fact_passport_blacklist
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_fact_passport_blacklist(
                            passport_num,
                            entry_dt)
                            SELECT
                            bl.passport_num,
                            bl.entry_dt
                            FROM de13ma.mrzn_stg_blacklist bl""")
    # загрузка из стейджинга в dwh_dim_fact_transactions
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_fact_transactions(
                             trans_id,
                             trans_date,
                             card_num,
                             oper_type,
                             amt,
                             oper_result,
                             terminal)
                             SELECT 
                             tr.trans_id,
                             tr.trans_date,
                             tr.card_num,
                             tr.oper_type,
                             tr.amt,
                             tr.oper_result,
                             tr.terminal
                             FROM  de13ma.mrzn_stg_transactions tr""")
    # формирование отчета за день
    report(conn_edu, cursor_edu, dt)
    # фиксация внесения данных в БД
    conn_edu.commit()
    # перенос файлов в архив
    for file in files:
        os.replace(f'data/{file}', f'archive/{file}.backup')
    # соединение с источниками больше не нужно, разрываем его
    disconnect(conn_edu, cursor_edu)


####################
# ETL - SCD1
def etl_scd1(files):
    # создание подключения к схеме edu, где лежат стейджинговые и целевые таблицы
    conn_edu = connect('edu', 'de-edu-db.chronosavant.ru',
                       'de13ma', 'meriadocbrandybuck', '5432')
    # создание курсора
    cursor_edu = conn_edu.cursor()

    # захват данных из источника (измененных с момента последней загрузки) в стейджинг
    # Работа со стейджинговыми таблицами
    # очистка стэйджинговых таблиц
    tables = ['transactions', 'accounts', 'cards', 'clients', 'terminals', 'blacklist']
    for table in tables:
        clear_table(f'stg_{table}', conn_edu, cursor_edu)
    # 1. загрузка данных из файлов - список формируется каждый день и
    # подается на вход функции в виде названий трех файлов - транзакции, терминалы и черный список
    # данные идут для загрузки в стейджинговые таблицы без предобработки, потом в архив'''
    # загрузка в таблицу транзакций за прошедший день

    # выгрузка из таблицы мета даты последнего обновления базы
    cursor_edu.execute('''SELECT max_update_dt
                                FROM de13ma.mrzn_stg_meta''')
    max_update_dt = cursor_edu.fetchone()[0]
    # print(max_update_dt)
    # выгрузка транзакций в датафрейм
    tr = pd.read_csv(f'data/{files[0]}', sep=';', decimal=',', header=0, index_col=None)
    tr['transaction_date'] = pd.to_datetime(tr['transaction_date'], format='%Y-%m-%d %H:%M:%S')
    # обновление таблицы мета максимальной датой пришедших транзакций для следующей загрузки
    dt = max(tr['transaction_date'])
    cursor_edu.execute("""UPDATE de13ma.mrzn_stg_meta
                                    SET max_update_dt = %s::timestamp""", (dt,))
    # фильтрация по max_update_dt, ранее выгруженной из мета
    tr = tr[tr['transaction_date'] > max_update_dt]
    # print(tr)
    # добавление отфильтрованного датафрейма в таблицу транзакций
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_transactions(
                                trans_id,
                                trans_date,
                                amt,
                                card_num,
                                oper_type,
                                oper_result,
                                terminal )
                                VALUES (%s, %s, %s, %s, %s, %s, %s)""", tr.values.tolist())
    # загрузка в таблицу терминалов за прошедший день
    term = pd.read_excel(f'data/{files[1]}', sheet_name='terminals', header=0, index_col=None)
    # print(term)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_terminals(
                                terminal_id,
                                terminal_type,
                                terminal_city,
                                terminal_address)
                                VALUES (%s, %s, %s, %s)""", term.values.tolist())

    # загрузка в таблицу черного списка за прошедший день и фильтрация по дате из мета
    blck = pd.read_excel(f'data/{files[2]}', sheet_name='blacklist', header=0, index_col=None)
    blck['date'] = pd.to_datetime(blck['date'], format='%Y-%m-%d %H:%M:%S')
    blck = blck[blck['date'] > max_update_dt]
    # print(blck)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_blacklist(
                                entry_dt,
                                passport_num)
                                VALUES (%s, %s)""", blck.values.tolist())
    # фиксация внесения данных в БД
    conn_edu.commit()
    # 2. загрузка стейджинговых таблиц из другого источника - обновляемой базы банка
    #  устанавливаем соединение с базой банка
    conn_bank = connect('bank', 'de-edu-db.chronosavant.ru',
                        'bank_etl', 'bank_etl_password', '5432')
    # создание курсора базы банк
    cursor_bank = conn_bank.cursor()

    # выгрузка данных из таблицы клиенты во временную переменную
    cursor_bank.execute("""SELECT client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone  
                                FROM info.clients""")
    clients = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    # выгрузка через пандас в стейджинговую таблицу клиентов из временной переменной
    clns = pd.DataFrame(clients, columns=names)
    cursor_edu.executemany(""" INSERT INTO de13ma.mrzn_stg_clients(  
                                client_id ,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone)
                                VALUES( %s, %s, %s, %s, %s, %s, %s, %s )""", clns.values.tolist())

    # выгрузка данных из базы банка по аккаунтам во временную переменную
    cursor_bank.execute("""SELECT account,
                                valid_to,
                                client 
                                FROM info.accounts""")
    accounts = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    # перегрузка данных из временной переменной в стейджинговую таблицу аккаунтов через пандас
    accnts = pd.DataFrame(accounts, columns=names)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_accounts(  
                                account_num,
                                valid_to,
                                client)
                                VALUES( %s, %s, %s )""", accnts.values.tolist())

    # загрузка через временную переменную данных по картам
    cursor_bank.execute("""SELECT card_num, 
                                account, 
                                create_dt, 
                                update_dt                            
                                FROM info.cards""")
    cards = cursor_bank.fetchall()
    names = [x[0] for x in cursor_bank.description]
    crds = pd.DataFrame(cards, columns=names)
    # print(crds)
    cursor_edu.executemany("""INSERT INTO de13ma.mrzn_stg_cards(  
                                card_num, 
                                account_num, 
                                create_dt, 
                                update_dt)                             
                                VALUES( %s, %s, %s,%s )""", crds.values.tolist())
    # подтверждение внесения изменений в стейджинговые таблицы
    conn_edu.commit()
    # соединение с источником bank больше не нужно, разрываем его
    disconnect(conn_bank, cursor_bank)

    # загрузка в приемник вставок на источнике passport_blacklist
    # в таблицу только добавляем данные! всех старых плохишей сохраняем!
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_fact_passport_blacklist(
                               passport_num,
                               entry_dt)
                               SELECT
                               bl.passport_num,
                               bl.entry_dt
                               FROM de13ma.mrzn_stg_blacklist bl
                               LEFT JOIN de13ma.mrzn_dwh_fact_passport_blacklist dbl
                               ON bl.passport_num = dbl.passport_num 
                               WHERE dbl.passport_num is NULL""")
    # захват данных из стейджинга в dwh_fact_transactions
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_fact_transactions(
                               trans_id,
                               trans_date,
                               card_num,
                               oper_type,
                               amt,
                               oper_result,
                               terminal)
                               SELECT 
                               tr.trans_id,
                               tr.trans_date,
                               tr.card_num, 
                               tr.oper_type,
                               tr.amt,
                               tr.oper_result,
                               tr.terminal 
                               FROM de13ma.mrzn_stg_transactions tr""")

    # доработка 29/04/23
    # перенос данных из стейджинга в dwh_accounts
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_accounts(
                                account_num,
                                valid_to,
                                client,
                                create_dt,
                                update_dt)
                                SELECT
                                ac.account_num,
                                ac.valid_to,
                                ac.client,
                                %s,
                                to_timestamp('9999-12-31','YYYY-MM-DD')
                                FROM de13ma.mrzn_stg_accounts ac
                                LEFT JOIN de13ma.mrzn_dwh_dim_accounts dac
                                ON ac.account_num = dac.account_num 
                                WHERE dac.account_num IS NULL""", (dt,))
    # перенос данных из стейджинга в dwh_dim_cards
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_cards(
                                card_num,
                                account_num,
                                create_dt,
                                update_dt)
                                SELECT
                                cr.card_num,
                                cr.account_num,
                                cr.create_dt,
                                cr.update_dt
                                FROM de13ma.mrzn_stg_cards cr
                                LEFT JOIN de13ma.mrzn_dwh_dim_cards dcr
                                ON cr.card_num=dcr.card_num
                                WHERE dcr.card_num IS NULL""")
    # перенос данных из стейджинга в dwh_dim_clients
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_clients(
                                client_id,
                                last_name,
                                first_name,
                                patronymic,
                                date_of_birth,
                                passport_num,
                                passport_valid_to,
                                phone,
                                create_dt,
                                update_dt)
                                SELECT
                                cl.client_id,
                                cl.last_name,
                                cl.first_name,
                                cl.patronymic,
                                cl.date_of_birth,
                                cl.passport_num,
                                cl.passport_valid_to,
                                cl.phone,
                                %s,
                                to_timestamp('9999-12-31','YYYY-MM-DD')
                                FROM de13ma.mrzn_stg_clients cl
                                LEFT JOIN de13ma.mrzn_dwh_dim_clients dcl
                                ON cl.client_id=dcl.client_id
                                WHERE dcl.client_id IS NULL""", (dt,))
    # перенос данных из стейджинга в dwh_dim_terminals
    cursor_edu.execute("""INSERT INTO de13ma.mrzn_dwh_dim_terminals(
                                 terminal_id,
                                 terminal_type,
                                 terminal_city,
                                 terminal_address,
                                 create_dt,
                                 update_dt)
                                 SELECT 
                                 tr.terminal_id,
                                 tr.terminal_type,
                                 tr.terminal_city,
                                 tr.terminal_address,
                                 %s,
                                 to_timestamp('9999-12-31','YYYY-MM-DD')
                                 FROM  de13ma.mrzn_stg_terminals tr
                                 LEFT JOIN de13ma.mrzn_dwh_dim_terminals dtr
                                 ON tr.terminal_id=dtr.terminal_id
                                 WHERE dtr.terminal_id IS NULL""", (dt,))
    # формирование отчета за день
    report(conn_edu, cursor_edu, dt)
    # подтверждение внесения изменений по предыдущим операциям
    conn_edu.commit()
    # перенос файлов в архив
    for file in files:
        os.replace(f'data/{file}', f'archive/{file}.backup')
    # соединение с источником edu больше не нужно, разрываем его
    disconnect(conn_edu, cursor_edu)


# список имен загружаемых файлов транзакций для обработки (для SCD1 и SCD2 одинаковый)
filess = [['transactions_01032021.txt', 'terminals_01032021.xlsx', 'passport_blacklist_01032021.xlsx'],
          ['transactions_02032021.txt', 'terminals_02032021.xlsx', 'passport_blacklist_02032021.xlsx'],
          ['transactions_03032021.txt', 'terminals_03032021.xlsx', 'passport_blacklist_03032021.xlsx']]
# вызов функции загрузки данных из разных источников на пустой стейджинг (для SCD1 и SCD2 одинаковая)
first_load(filess[0])
# функция инкрементальной загрузки SCD1
for files in filess[1:]:
    etl_scd1(files)
