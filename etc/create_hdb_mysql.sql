CREATE TABLE IF NOT EXISTS adt
(
ID smallint(5) unsigned zerofill not null auto_increment,
time datetime,
full_name varchar(200) not null default '',
device varchar(150) not null default '',
domain varchar(35) not null default '',
family varchar(35) not null default '',
member varchar(35) not null default '',
att_name varchar(50) not null default '',
data_type tinyint(1) not null default 0,
data_format tinyint(1) not null default 0,
writable tinyint(1) not null default 0,
max_dim_x smallint(6) unsigned not null default 0,
max_dim_y smallint(6) unsigned not null default 0,
levelg tinyint(1) not null default 0,
facility varchar(45) not null default '',
archivable tinyint(1) not null default 0,
substitute smallint(9) not null default 0,
PRIMARY KEY (ID, full_name)
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS amt
(
ID smallint(5) unsigned zerofill not null default 00000,
archiver varchar(255) not null default '',
start_date datetime default NULL,
stop_date datetime default NULL,
per_mod int(1) not null default 0,
per_per_mod int(5) default NULL,
abs_mod int(1) not null default 0,
per_abs_mod int(5) default NULL,
dec_del_abs_mod double default NULL,
gro_del_abs_mod double default NULL,
rel_mod int(1) not null default 0,
per_rel_mod int(5) default NULL,
n_percent_rel_mod double default NULL,
p_percent_rel_mod double default NULL,
thr_mod int(1) not null default 0,
per_thr_mod int(5) default NULL,
min_val_thr_mod double default NULL,
max_val_thr_mod double default NULL,
cal_mod int(1) not null default 0,
per_cal_mod int(5) default NULL,
val_cal_mod int(3) default NULL,
type_cal_mod int(2) default NULL,
algo_cal_mod varchar(20) default NULL,
dif_mod int(1) not null default 0,
per_dif_mod int(5) default NULL,
ext_mod int(1) not null default 0,
refresh_mode tinyint(4) default 0
) ENGINE=MyISAM;

CREATE TABLE IF NOT EXISTS apt
(
ID int(5) unsigned zerofill not null default 00000,
time datetime default NULL,
description varchar(255) not null default '',
label varchar(64) not null default '',
unit varchar(64) not null default '1',
standard_unit varchar(64) not null default '1',
display_unit varchar(64) not null default '',
format varchar(64) not null default '',
min_value varchar(64) not null default '0',
max_value varchar(64) not null default '0',
min_alarm varchar(64) not null default '0',
max_alarm varchar(64) not null default '0',
PRIMARY KEY (ID)
) ENGINE=MyISAM;

