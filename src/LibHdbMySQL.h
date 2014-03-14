//=============================================================================
//
// file :        LibHdbMySQL.h
//
// description : Include for the LibHdbMySQL library.
//
// Author: Graziano Scalamera
//
// $Revision: 1.5 $
//
// $Log: LibHdbMySQL.h,v $
// Revision 1.5  2014-03-07 13:13:00  graziano
// removed _HDBEVENTDATA
//
// Revision 1.4  2014-02-20 14:12:30  graziano
// name and path fixing,
// development of prepared stmt for insert
// development of configuration query
//
// Revision 1.3  2013-09-24 08:48:44  graziano
// support for HdbEventData
//
// Revision 1.2  2013-09-02 12:24:34  graziano
// libhdb refurbishing
//
// Revision 1.1  2013-08-23 10:07:20  graziano
// first commit
//
//
//
//=============================================================================

#ifndef _HDB_MYSQL_H
#define _HDB_MYSQL_H

#include <mysql.h>
#include "LibHdb++.h"

#include <string>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <stdint.h>

//Tango:
#include <tango.h>
//#include <event.h>

//######## adt ########
#define ADT_TABLE_NAME			"adt"
#define ADT_COL_ID				"ID"
#define ADT_COL_DATA_TYPE		"data_type"
#define ADT_COL_DATA_FORMAT		"data_format"
#define ADT_COL_WRITABLE		"writable"
#define ADT_COL_FULL_NAME		"full_name"
#define ADT_COL_FACILITY		"facility"
#define ADT_COL_TIME			"time"

//######## amt ########
#define AMT_TABLE_NAME			"amt"
#define AMT_COL_ID				"ID"
#define AMT_COL_STARTDATE		"start_date"
#define AMT_COL_STOPDATE		"stop_date"

//######## att_xxx ########
#define ATT_TABLE_NAME			"att_"
#define ATT_COL_TIME			"time"
#define ATT_COL_DIMX			"dim_x"
#define ATT_COL_VALUE_RO		"value"
#define ATT_COL_R_VALUE_RW		"read_value"
#define ATT_COL_W_VALUE_RW		"write_value"

enum Attr_Type
{
	scalar_double_ro,
	scalar_double_rw,
	array_double_ro,
	array_double_rw,
	scalar_string_ro,
	scalar_string_rw,
	array_string_ro,
	array_string_rw,
	unknown
};

class HdbMySQL : public AbstractDB
{
private:

	MYSQL *dbp;
	string m_dbname;
	map<string,int> attr_ID_map;
	

	int store_double_RO(string attr, vector<double> value, double time);
	int store_double_RW(string attr, vector<double> value_r, vector<double> value_W, double time);
	int store_string_RO(string attr, vector<string> value, double time);
	int store_string_RW(string attr, vector<string> value_r, vector<string> value_w, double time);
	
	string get_only_attr_name(string str);
	string get_only_tango_host(string str);
	string remove_domain(string facility);

	Attr_Type get_attr_type(int data_type, int data_format, int writable);

public:

	~HdbMySQL();

	HdbMySQL(string host, string user, string password, string dbname, int port);  

	//void connect_db(string host, string user, string password, string dbname);

	int find_attr_id(string facility, string attr_name, int &ID);
	int find_attr_id_type(string facility, string attr_name, int &ID, int data_type, int data_format, int writable);
	virtual int insert_Attr(Tango::EventData *data);
	virtual int configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/);
	virtual int remove_Attr(string name);
	virtual int start_Attr(string name);
	virtual int stop_Attr(string name);
};

class HdbMySQLFactory : public DBFactory
{

public:
	virtual AbstractDB* create_db(string host, string user, string password, string dbname, int port);

};

#endif

