-----------------------------------------
/* создание стейджинговых таблиц по тз */

-- таблица транзакций
create table mrzn_stg_transactions (
	trans_id varchar(20) null,
	trans_date timestamp(0) null,
	card_num varchar(20) null,
	oper_type varchar(12) null,
	amt numeric  null,
	oper_result varchar(10) null,
	terminal varchar(12) null
);

-- таблица терминалов
create table mrzn_stg_terminals (
	terminal_id varchar(10) null,
	terminal_type varchar(10) null,
	terminal_city varchar(30) null,
	terminal_address varchar(120) null
);

-- таблица черного списка
create table mrzn_stg_blacklist (
	passport_num varchar(30) null,
	entry_dt date null
);

-- таблица карт
create table mrzn_stg_cards (
	card_num varchar(20) null,
	account_num varchar(20)  null,
	create_dt timestamp(0) null,
	update_dt timestamp(0) null
);

-- таблица аккаунтов
create table mrzn_stg_accounts (
	account_num varchar(20) null,
	valid_to date null,
	client varchar(10) null
);

-- таблица клиентов
create table mrzn_stg_clients (
	client_id varchar(10) null,
	last_name varchar(20) null,
	first_name varchar(20) null,
	patronymic varchar(20) null,
	date_of_birth date null,
	passport_num varchar(18) null,
	passport_valid_to date null,
	phone varchar(16) null
);	

-- таблица для отслежвания изменений - мета
create table mrzn_stg_meta(
	schema_name varchar(30),
   	table_name varchar(30),
	max_update_dt timestamp(0)
);

-- заполнение первой записью таблицы мета
insert into mrzn_stg_meta(schema_name, table_name, max_update_dt)
	values('de13ma','mrzn_stg_transactions', to_timestamp('1900-01-01','YYYY-MM-DD')
);

-----------------------------------------
/* создание DWH таблиц */

-- таблица фактов-транзакций
create table mrzn_dwh_fact_transactions (
	trans_id varchar(20) null,
	trans_date timestamp(0) null,
	card_num varchar(20) null,
	oper_type varchar(12) null,
	amt numeric  null,
	oper_result varchar(10) null,
	terminal varchar(12) null   
);

-- таблица фактов-паспортных данных из черного списка
create table mrzn_dwh_fact_passport_blacklist (
	passport_num varchar(30) null,
	entry_dt date null 
);

-- таблица измерений-терминалы
create table mrzn_dwh_dim_terminals (
	terminal_id varchar(10) null,
	terminal_type varchar(10) null,
	terminal_city varchar(30) null,
	terminal_address varchar(120) null,
	create_dt timestamp(0) null,
	update_dt timestamp(0) null
);

-- таблица измерений-карты
create table mrzn_dwh_dim_cards (
	card_num varchar(20) null,
	account_num varchar(20) null,
	create_dt timestamp(0) null,
	update_dt timestamp(0) null
);

-- таблица измерений-аккаунты
create table mrzn_dwh_dim_accounts (
  	account_num varchar(20) null,
	valid_to date null,
	client varchar(10) null,
	create_dt timestamp(0) null,
	update_dt timestamp(0) null
);

-- таблица измерений-клиенты
create table mrzn_dwh_dim_clients (
	client_id varchar(10) null,
	last_name varchar(20) null,
	first_name varchar(20) null,
	patronymic varchar(20) null,
	date_of_birth date null,
	passport_num varchar(20) null,
	passport_valid_to date null,
	phone varchar(16) null,
	create_dt timestamp(0) null,
	update_dt timestamp(0) null  
);

-- таблица-отчет о мошеннических транзакциях
create table mrzn_rep_froud (
	event_dt timestamp(0),
	passport varchar(20),
	fio varchar(50),
	phone varchar(16),
	event_type varchar(120),
	report_dt timestamp(0)
);

-----------------------------------------
/* подтверждение операции по созданию таблиц */

commit;


------------------------------------------
/* блок для отладки кода */
--очистка всех таблиц
delete from de13ma.mrzn_rep_fraud;
delete from de13ma.mrzn_stg_accounts ;
delete from de13ma.mrzn_stg_blacklist ;
delete from de13ma.mrzn_stg_cards ;
delete from de13ma.mrzn_stg_clients ;
delete from de13ma.mrzn_stg_meta ;
delete from de13ma.mrzn_stg_terminals ;
delete from de13ma.mrzn_stg_transactions ;

delete from de13ma.mrzn_dwh_dim_accounts ;
delete from de13ma.mrzn_dwh_dim_cards ;
delete from de13ma.mrzn_dwh_dim_clients ;
delete from de13ma.mrzn_dwh_dim_terminals ;
delete from de13ma.mrzn_dwh_fact_passport_blacklist ;
delete from de13ma.mrzn_dwh_fact_transactions ;

-- заполнение первой записью таблицы мета
insert into mrzn_stg_meta(schema_name, table_name, max_update_dt)
	values('de13ma','mrzn_stg_transactions', to_timestamp('1900-01-01','YYYY-MM-DD')
);
commit;
