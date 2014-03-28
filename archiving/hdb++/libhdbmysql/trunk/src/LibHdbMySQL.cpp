//=============================================================================
//
// file :        LibHdbMySQL.cpp
//
// description : Source file for the LibHdbMySQL library.
//
// Author: Graziano Scalamera
//
// $Revision: 1.6 $
//
// $Log: LibHdbMySQL.cpp,v $
// Revision 1.6  2014-03-07 13:13:00  graziano
// removed _HDBEVENTDATA
//
// Revision 1.5  2014-02-20 14:12:30  graziano
// name and path fixing,
// development of prepared stmt for insert
// development of configuration query
//
// Revision 1.4  2013-09-24 08:48:44  graziano
// support for HdbEventData
//
// Revision 1.3  2013-09-02 12:38:47  graziano
// fixed release string
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

#include "LibHdbMySQL.h"
#include <stdlib.h>
#include <mysql.h>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <cmath>

#ifndef LIB_BUILDTIME
#define LIB_BUILDTIME   RELEASE " " __DATE__ " "  __TIME__
#endif

const char version_string[] = "$Build: " LIB_BUILDTIME " $";
static const char __FILE__rev[] = __FILE__ " $Revision: 1.6 $";

//#define _LIB_DEBUG

HdbMySQL::HdbMySQL(string host, string user, string password, string dbname, int port)
{
	m_dbname = dbname;
	dbp = new MYSQL();
	if(!mysql_init(dbp))
	{
		cout << __func__<<": VERSION: " << version_string << " file:" << __FILE__rev << endl;
		cout << __func__<< ": mysql init db error: "<< mysql_error(dbp) << endl;
	}
	my_bool my_auto_reconnect=1;
	if(mysql_options(dbp,MYSQL_OPT_RECONNECT,&my_auto_reconnect) !=0)
	{
		cout << __func__<<": mysql auto reconnection error: " << mysql_error(dbp) << endl;
	}



	if(!mysql_real_connect(dbp, host.c_str(), user.c_str(), password.c_str(), dbname.c_str(), port, NULL, 0))
	{
		cout << __func__<< ": mysql connect db error: "<< mysql_error(dbp) << endl;
	}
	else
	{
		//everything OK
#ifdef _LIB_DEBUG
		cout << __func__<< ": mysql connection OK" << endl;
#endif
	}

}

HdbMySQL::~HdbMySQL()
{
	mysql_close(dbp);
	delete dbp;
}

int HdbMySQL::find_attr_id(string facility, string attr, int &ID)
{
	string complete_name = string("tango://") + facility + string("/") + attr;
	map<string,int>::iterator it = attr_ID_map.find(complete_name);

	if(it != attr_ID_map.end())
	{
		ID = it->second;
		return 0;
	}
	//if not already present in cache, look for ID in the DB


	ostringstream query_str;
	string facility_no_domain = remove_domain(facility);


	//TODO: concatenate facility and full_name? ... WHERE CONCAT(ADT_COL_FACILITY, ADT_COL_FULL_NAME) IN (...)
	query_str << 
		"SELECT " << ADT_COL_ID << " FROM " << m_dbname << "." << ADT_TABLE_NAME <<
			" WHERE LOWER(" << ADT_COL_FULL_NAME << ")='" <<attr << "' AND LOWER(" <<
				ADT_COL_FACILITY << ") IN ('" << facility << "','" << facility_no_domain << "')";
	
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << " err="<<mysql_error(dbp)<< endl;
		return -1;
	}
	else
	{
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{
			cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
			if(num_found > 0)
			{
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
			}
			else
			{
				cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
				mysql_free_result(res);
				return -1;
			}
		}
#endif
		while ((row = mysql_fetch_row(res)))
		{
			ID = atoi(row[0]);
		}	
		mysql_free_result(res);
		if(ID != -1)
		{
			attr_ID_map.insert(make_pair(complete_name,ID));
		}
	}
	return 0;
}

Attr_Type HdbMySQL::get_attr_type(int data_type, int data_format, int writable)
{
	if(data_type != Tango::DEV_STRING && data_format == Tango::SCALAR && writable == Tango::READ)
	{
		return scalar_double_ro;
	}
	else if(data_type != Tango::DEV_STRING && data_format == Tango::SCALAR && writable == Tango::READ_WRITE)
	{
		return scalar_double_rw;
	}
	else if(data_type != Tango::DEV_STRING && data_format == Tango::SPECTRUM && writable == Tango::READ)
	{
		return array_double_ro;
	}
	else if(data_type != Tango::DEV_STRING && data_format == Tango::SPECTRUM && writable == Tango::READ_WRITE)
	{
		return array_double_rw;
	}
	else if(data_type == Tango::DEV_STRING && data_format == Tango::SCALAR && writable == Tango::READ)
	{
		return scalar_string_ro;
	}
	else
	{
		return unknown;	//TODO: verify other types: string rw, string array, encoded, ...
	}
}

int HdbMySQL::find_attr_id_type(string facility, string attr, int &ID, int data_type, int data_format, int writable)
{
	ostringstream query_str;
	string facility_no_domain = remove_domain(facility);
	int db_data_type, db_data_format, db_writable;

	//TODO: concatenate facility and full_name? ... WHERE CONCAT(ADT_COL_FACILITY, ADT_COL_FULL_NAME) IN (...)
	query_str << 
		"SELECT " << ADT_COL_ID << "," << ADT_COL_DATA_TYPE << "," << ADT_COL_DATA_FORMAT << "," << ADT_COL_WRITABLE <<
			" FROM " << m_dbname << "." << ADT_TABLE_NAME <<
			" WHERE LOWER(" << ADT_COL_FULL_NAME << ")='" <<attr << "' AND LOWER(" << 
				ADT_COL_FACILITY << ") IN ('" << facility << "','" << facility_no_domain << "')";
	
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << " err="<<mysql_error(dbp)<< endl;
		return -1;
	}
	else
	{
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{
			cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
			if(num_found > 0)
			{
				cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
			}
			else
			{
				cout << __func__<< ": NO RESULT in query: " << query_str.str() << endl;
				mysql_free_result(res);
				return -1;
			}
		}
#endif
		bool found = false;
		while ((row = mysql_fetch_row(res)))
		{
			found = true;
			ID = atoi(row[0]);
			db_data_type = atoi(row[1]);
			db_data_format = atoi(row[2]);
			db_writable = atoi(row[3]);
		}
		mysql_free_result(res);
		if(!found)
			return -1;

		if(get_attr_type(data_type, data_format, writable) != get_attr_type(db_data_type, db_data_format, db_writable))
		{
			cout << __func__<< ": FOUND ID="<<ID<<" but different type: data_type="<<data_type<<"-db_data_type="<<db_data_type<<
					" data_format="<<data_format<<"-db_data_format="<<db_data_format<<" writable="<<writable<<"-db_writable="<<db_writable<< endl;
			return -2;
		}
		else
		{
			cout << __func__<< ": FOUND ID="<<ID<<" with SAME type: data_type="<<data_type<<"-db_data_type="<<db_data_type<<
					" data_format="<<data_format<<"-db_data_format="<<db_data_format<<" writable="<<writable<<"-db_writable="<<db_writable<< endl;
			return 0;
		}
	}
	return 0;
}

int HdbMySQL::insert_Attr(Tango::EventData *data, HdbEventDataType ev_data_type)
{
	int ret = -1;
#ifdef _LIB_DEBUG
	cout << __func__<< ": entering..." << endl;
#endif
	try
	{
		string attr_name = data->attr_name; //TODO: fqdn!!

		double	time;
		vector<double>	dval;
		vector<double>	dval_r;
		vector<double>	dval_w;
#if 0
		Tango::AttributeDimension attr_w_dim = data->attr_value->get_w_dimension();
		Tango::AttributeDimension attr_r_dim = data->attr_value->get_r_dimension();
		int data_type = data->attr_value->get_type();
		//Tango::AttrDataFormat data_format = data->attr_value->get_data_format();	//works if bug 627 fixed
		Tango::AttrDataFormat data_format = (attr_w_dim.dim_x <= 1 && attr_w_dim.dim_y <= 1 && attr_r_dim.dim_x <= 1 && attr_r_dim.dim_y <= 1) ? \
				Tango::SCALAR : Tango::SPECTRUM;	//TODO
		int write_type = (attr_w_dim.dim_x == 0 && attr_w_dim.dim_y == 0) ? Tango::READ : Tango::READ_WRITE;	//TODO
#else
		//Tango::AttributeDimension attr_w_dim = ev_data_type.attr_w_dim;
		//Tango::AttributeDimension attr_r_dim = ev_data_type.attr_r_dim;
		int data_type = ev_data_type.data_type;
		Tango::AttrDataFormat data_format = ev_data_type.data_format;
		int write_type = ev_data_type.write_type;
		int max_dim_x = ev_data_type.max_dim_x;
		int max_dim_y = ev_data_type.max_dim_y;
#endif

		bool isNull = false;
		if(data->err || data->attr_value->is_empty() || data->attr_value->get_quality() == Tango::ATTR_INVALID)
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": going to archive as NULL..." << endl;
#endif
			isNull = true;
		}
#ifdef _LIB_DEBUG
		cout << __func__<< ": data_type="<<data_type<<" data_format="<<data_format<<" write_type="<<write_type << " max_dim_x="<<max_dim_x<<" max_dim_y="<<max_dim_y<< endl;
#endif

		if(!isNull)
		{
			time = data->attr_value->get_date().tv_sec + (double)data->attr_value->get_date().tv_usec/1.0e6;		//event time
#ifdef _LIB_DEBUG
//			Tango::AttributeDimension attr_w_dim = data->attr_value->get_w_dimension();
//			Tango::AttributeDimension attr_r_dim = data->attr_value->get_r_dimension();
//			cout << __func__<< ": r_dim_x="<<attr_r_dim.dim_x<<" r_dim_y="<<attr_r_dim.dim_y<<" w_dim_x="<<attr_w_dim.dim_x << " w_dim_y="<<attr_w_dim.dim_y<< endl;
#endif
		}
		else
		{
			time = data->get_date().tv_sec + (double)data->get_date().tv_usec/1.0e6;	//receive time
		}


		switch(data_type)
		{
		case Tango::DEV_DOUBLE:
		{
			
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(dval))
				{
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(dval_r) && data->attr_value->extract_set(dval_w))
				{
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_FLOAT:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<float>	fval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(fval))
				{
					for (unsigned int i=0 ; i<fval.size() ; i++)
						dval.push_back((double)fval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<float>	fval_r;
				vector<float>	fval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(fval_r) && data->attr_value->extract_set(fval_w))
				{
					for (unsigned int i=0 ; i<fval_r.size() ; i++)
						dval_r.push_back((double)fval_r[i]);
					for (unsigned int i=0 ; i<fval_w.size() ; i++)
						dval_w.push_back((double)fval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_LONG:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevLong>	lval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(lval))
				{
					for (unsigned int i=0 ; i<lval.size() ; i++)
						dval.push_back((double)lval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevLong>	lval_r;
				vector<Tango::DevLong>	lval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(lval_r) && data->attr_value->extract_set(lval_w))
				{
					for (unsigned int i=0 ; i<lval_r.size() ; i++)
						dval_r.push_back((double)lval_r[i]);
					for (unsigned int i=0 ; i<lval_w.size() ; i++)
						dval_w.push_back((double)lval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_ULONG:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevULong>	ulval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(ulval))
				{
					for (unsigned int i=0 ; i<ulval.size() ; i++)
						dval.push_back((double)ulval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevULong>	ulval_r;
				vector<Tango::DevULong>	ulval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(ulval_r) && data->attr_value->extract_set(ulval_w))
				{
					for (unsigned int i=0 ; i<ulval_r.size() ; i++)
						dval_r.push_back((double)ulval_r[i]);
					for (unsigned int i=0 ; i<ulval_w.size() ; i++)
						dval_w.push_back((double)ulval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_LONG64:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevLong64>	l64val;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(l64val))
				{
					for (unsigned int i=0 ; i<l64val.size() ; i++)
						dval.push_back((double)l64val[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevLong64>	l64val_r;
				vector<Tango::DevLong64>	l64val_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(l64val_r) && data->attr_value->extract_set(l64val_w))
				{
					for (unsigned int i=0 ; i<l64val_r.size() ; i++)
						dval_r.push_back((double)l64val_r[i]);
					for (unsigned int i=0 ; i<l64val_w.size() ; i++)
						dval_w.push_back((double)l64val_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_ULONG64:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevULong64>	ul64val;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(ul64val))
				{
					for (unsigned int i=0 ; i<ul64val.size() ; i++)
						dval.push_back((double)ul64val[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevULong64>	ul64val_r;
				vector<Tango::DevULong64>	ul64val_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(ul64val_r) && data->attr_value->extract_set(ul64val_w))
				{
					for (unsigned int i=0 ; i<ul64val_r.size() ; i++)
						dval_r.push_back((double)ul64val_r[i]);
					for (unsigned int i=0 ; i<ul64val_w.size() ; i++)
						dval_w.push_back((double)ul64val_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_SHORT:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevLong>	lval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(lval))
				{
					for (unsigned int i=0 ; i<lval.size() ; i++)
						dval.push_back((double)lval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevShort>	sval_r;
				vector<Tango::DevShort>	sval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(sval_r) && data->attr_value->extract_set(sval_w))
				{
					for (unsigned int i=0 ; i<sval_r.size() ; i++)
						dval_r.push_back((double)sval_r[i]);
					for (unsigned int i=0 ; i<sval_w.size() ; i++)
						dval_w.push_back((double)sval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_USHORT:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevUShort>	usval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(usval))
				{
					for (unsigned int i=0 ; i<usval.size() ; i++)
						dval.push_back((double)usval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevUShort>	usval_r;
				vector<Tango::DevUShort>	usval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(usval_r) && data->attr_value->extract_set(usval_w))
				{
					for (unsigned int i=0 ; i<usval_r.size() ; i++)
						dval_r.push_back((double)usval_r[i]);
					for (unsigned int i=0 ; i<usval_w.size() ; i++)
						dval_w.push_back((double)usval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_BOOLEAN:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevBoolean>	bval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(bval))
				{
					for (unsigned int i=0 ; i<bval.size() ; i++)
						dval.push_back((double)bval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevBoolean>	bval_r;
				vector<Tango::DevBoolean>	bval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(bval_r) && data->attr_value->extract_set(bval_w))
				{
					for (unsigned int i=0 ; i<bval_r.size() ; i++)
						dval_r.push_back((double)bval_r[i]);
					for (unsigned int i=0 ; i<bval_w.size() ; i++)
						dval_w.push_back((double)bval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_UCHAR:
		{
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<Tango::DevUChar>	ucval;
				if(isNull)
				{
					dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(ucval))
				{
					for (unsigned int i=0 ; i<ucval.size() ; i++)
						dval.push_back((double)ucval[i]);
					ret = store_double_RO(attr_name, dval, time, data_format);
				}
				else
				{
					if(dval.size() == 0)
						dval.push_back(0);//fake value
					ret = store_double_RO(attr_name, dval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<Tango::DevUChar>	ucval_r;
				vector<Tango::DevUChar>	ucval_w;
				if(isNull)
				{
					dval_r.push_back(0);//fake value
					dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(ucval_r) && data->attr_value->extract_set(ucval_w))
				{
					for (unsigned int i=0 ; i<ucval_r.size() ; i++)
						dval_r.push_back((double)ucval_r[i]);
					for (unsigned int i=0 ; i<ucval_w.size() ; i++)
						dval_w.push_back((double)ucval_w[i]);
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format);
				}
				else
				{
					if(dval_r.size() == 0)
						dval_r.push_back(0);//fake value
					if(dval_w.size() == 0)
						dval_w.push_back(0);//fake value
					ret = store_double_RW(attr_name, dval_r, dval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_STRING:
		{
			//TODO
//			extract values and get only read ones.
			if(write_type == Tango::READ)
			{
				vector<string>	ssval;
				if(isNull)
				{
					ssval.push_back(0);//fake value
					ret = store_string_RO(attr_name, ssval, time, data_format, isNull);
				}
				else if (data->attr_value->extract_read(ssval))
				{
					ret = store_string_RO(attr_name, ssval, time, data_format);
				}
				else
				{
					if(ssval.size() == 0)
						ssval.push_back(0);//fake value
					ret = store_string_RO(attr_name, ssval, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			else
			{
				vector<string>	ssval_r;
				vector<string>	ssval_w;
				if(isNull)
				{
					ssval_r.push_back(0);//fake value
					ssval_w.push_back(0);//fake value
					ret = store_string_RW(attr_name, ssval_r, ssval_w, time, data_format, isNull);
				}
				else if(data->attr_value->extract_read(ssval_r) && data->attr_value->extract_set(ssval_w))
				{
					ret = store_string_RW(attr_name, ssval_r, ssval_w, time, data_format);
				}
				else
				{
					if(ssval_r.size() == 0)
						ssval_r.push_back(0);//fake value
					if(ssval_w.size() == 0)
						ssval_w.push_back(0);//fake value
					ret = store_string_RW(attr_name, ssval_r, ssval_w, time, data_format, true);
					cout << __func__<<": failed to extract " << attr_name << endl;
					return -1;
				}
			}
			break;
		}
		case Tango::DEV_STATE:
		{
			if(isNull)
			{
				dval.push_back(0);//fake value
				ret = store_double_RO(attr_name, dval, time, data_format, isNull);
			}
			else
			{
				Tango::DevState	st;
				*data->attr_value >> st;
				dval.push_back((double)st);
				ret = store_double_RO(attr_name, dval, time, data_format);
			}
			break;
		}
		default:
		{
			TangoSys_MemStream	os;
			os << "Attribute " << data->attr_name<< " type (" << (int)data_type << ") not supported";
			cout << os.str() << endl;
			return -1;
		}
		}
	}
	catch(Tango::DevFailed &e)
	{

		cout << __func__<<": Exception on " << data->attr_name << ":" << endl;
		
		for (unsigned int i=0; i<e.errors.length(); i++)
		{
			cout << e.errors[i].reason << endl;
			cout << e.errors[i].desc << endl;
			cout << e.errors[i].origin << endl;
		}
 
		cout << endl;	
	}
#ifdef _LIB_DEBUG
//	cout << __func__<< ": exiting... ret="<<ret << endl;
#endif
	return ret;
}

int HdbMySQL::store_double_RO(string attr, vector<double> value, double time, Tango::AttrDataFormat data_format, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif

	int ID=-1;
	string facility = get_only_tango_host(attr);
	string attr_name = get_only_attr_name(attr);
	find_attr_id(facility, attr_name, ID);
	if(ID == -1)
	{

		cout << __func__<< ": ID not found!" << endl;
		return -1;
	}

	ostringstream query_str;
	char attr_tbl_name[64];
	sprintf(attr_tbl_name,"%s%05d",ATT_TABLE_NAME,ID);
	int timestamp = (int)(time);	//TODO
	  //example: 
	  //INSERT INTO attr_12345
	  //	(a/b/c/d, e/f/g/h)

//#define _NOT_BIND 1
	  //TODO: concatenate facility and full_name? ... WHERE CONCAT(ADT_COL_FACILITY, ADT_COL_FULL_NAME) IN (...)
//#if _NOT_BIND
	if(data_format != Tango::SCALAR)	//array
	{
		query_str <<
			"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
				" (" << ATT_COL_TIME << ",";
				if(data_format != Tango::SCALAR)
					query_str << ATT_COL_DIMX << ",";
				query_str << ATT_COL_VALUE_RO << ")" <<
				" VALUES (FROM_UNIXTIME(" << timestamp << "),";
				if(data_format != Tango::SCALAR)
					query_str << value.size() << ",'";
				query_str <<std::scientific<<setprecision(16);
				for(uint32_t i=0; i<value.size(); i++)
				{
					if((!(std::isnan(value[i]) || std::isinf(value[i])) || data_format != Tango::SCALAR)  && !isNull )	//TODO: ok nan and inf if spectrum?
						query_str << value[i];
					else
						query_str << "NULL";		//TODO: MySql supports nan and inf?
					if(i != value.size()-1)
						query_str << ", ";
				}
				if(data_format != Tango::SCALAR)
					query_str << "'";
				query_str << ")";

		if(mysql_query(dbp, query_str.str().c_str()))
		{
			cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
			return -1;
		}
		else
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		}
	}
	else
	{
		my_bool		is_null;    /* value nullability */
		query_str <<
			"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
				" (" << ATT_COL_TIME << ",";
				query_str << ATT_COL_VALUE_RO << ")" <<
				" VALUES (FROM_UNIXTIME(" << timestamp << "),";
				//query_str <<std::scientific<<setprecision(16);
					query_str << "?";
					if(value.size() < 1 || std::isnan(value[0]) || std::isinf(value[0]) || isNull)	//TODO: MySql supports nan and inf?
						is_null = 1;
					else
						is_null = 0;
				query_str << ")";

		MYSQL_STMT    *pstmt;
		MYSQL_BIND    *plog_bind = new MYSQL_BIND[1];
		//int           param_count;
		pstmt = mysql_stmt_init(dbp);
		if (!pstmt)
		{
			cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		}
		if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
		{
			cout << __func__<< ": mysql_stmt_prepare(), INSERT failed" << endl;
		}

		/*param_count= mysql_stmt_param_count(pstmt);

		if (param_count != value.size())
		{
			cout << __func__<< ": invalid parameter count returned by MySQL" << endl;
		}*/
		memset(plog_bind, 0, sizeof(MYSQL_BIND));

		plog_bind[0].buffer_type= MYSQL_TYPE_DOUBLE;
		if(value.size() == 1)
			plog_bind[0].buffer= (void *)&value[0];
		plog_bind[0].is_null= &is_null;
		plog_bind[0].length= 0;
		
		if (mysql_stmt_bind_param(pstmt, plog_bind))
			cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;

		if (mysql_stmt_execute(pstmt))
		{
			cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
			delete [] plog_bind;
			if (mysql_stmt_close(pstmt))
				cout << __func__<< ": failed while closing the statement" << endl;
			return -1;
		}
		else
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		}
		delete [] plog_bind;

	/*	if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
	}

	return 0;
}

int HdbMySQL::store_double_RW(string attr, vector<double> value_r, vector<double> value_w, double time, Tango::AttrDataFormat data_format, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	int ID=-1;
	string facility = get_only_tango_host(attr);
	string attr_name = get_only_attr_name(attr);
	find_attr_id(facility, attr_name, ID);
	if(ID == -1)
	{

		cout << __func__<< ": ID not found!" << endl;
		return -1;
	}

	ostringstream query_str;
	char attr_tbl_name[64];
	sprintf(attr_tbl_name,"%s%05d",ATT_TABLE_NAME,ID);
	int timestamp = (int)(time);	//TODO
  //example:
  //INSERT INTO attr_12345
  //	(a/b/c/d, e/f/g/h)


	if(data_format != Tango::SCALAR)	//array
	{
		int max_size = (value_r.size() > value_w.size()) ? value_r.size() : value_w.size();
#ifdef _LIB_DEBUG
		cout << __func__<< ": value_r.size()="<<value_r.size()<<" value_w.size()="<<value_w.size()<<" max_size="<<max_size << endl;
#endif
		query_str <<
			"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
				" (" << ATT_COL_TIME << ",";
				if(data_format != Tango::SCALAR)
					query_str << ATT_COL_DIMX << ",";
				query_str << ATT_COL_R_VALUE_RW << "," << ATT_COL_W_VALUE_RW << ")" <<
				" VALUES (FROM_UNIXTIME(" << timestamp << "),";
				if(data_format != Tango::SCALAR)
					query_str << max_size << ",'";
				query_str <<std::scientific<<setprecision(16);
				for(uint32_t i=0; i<max_size; i++)
				{
					if((!(std::isnan(value_r[i]) || std::isinf(value_r[i])) || data_format != Tango::SCALAR) && i<value_r.size() && !isNull)	//TODO: ok nan and inf if spectrum?
						query_str << value_r[i];
					else
						query_str << "NULL";		//TODO: which MySql supports nan and inf?
					if(i != max_size-1)
						query_str << ", ";
				}
				if(data_format != Tango::SCALAR)
					query_str << "','";
				else
					query_str << ",";
				for(uint32_t i=0; i<max_size; i++)
				{
					if((!(std::isnan(value_w[i]) || std::isinf(value_w[i])) ||  data_format != Tango::SCALAR) && i<value_w.size() && !isNull)	//TODO: ok nan and inf if spectrum?
						query_str << value_w[i];
					else
						query_str << "NULL";		//TODO: which MySql supports nan and inf?
					if(i != max_size-1)
						query_str << ", ";
				}
				if(data_format != Tango::SCALAR)
					query_str << "'";
				query_str << ")";


		if(mysql_query(dbp, query_str.str().c_str()))
		{
			cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
			return -1;
		}
		else
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		}
	}
	else
	{
		my_bool		is_null[2];    /* value nullability */
		query_str <<
			"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
				" (" << ATT_COL_TIME << ",";
				query_str << ATT_COL_R_VALUE_RW << "," << ATT_COL_W_VALUE_RW << ")" <<
				" VALUES (FROM_UNIXTIME(" << timestamp << "),";
				//query_str <<std::scientific<<setprecision(16);
					query_str << "?,?";
					if(value_r.size() < 1 || std::isnan(value_r[0]) || std::isinf(value_r[0]) || isNull)	//TODO: MySql supports nan and inf?
						is_null[0] = 1;
					else
						is_null[0] = 0;
					if(value_w.size() < 1 || std::isnan(value_w[0]) || std::isinf(value_w[0]) || isNull)	//TODO: MySql supports nan and inf?
						is_null[1] = 1;
					else
						is_null[1] = 0;
				query_str << ")";

		MYSQL_STMT    *pstmt;
		MYSQL_BIND    *plog_bind = new MYSQL_BIND[2];
		//int           param_count;
		pstmt = mysql_stmt_init(dbp);
		if (!pstmt)
		{
			cout << __func__<< ": mysql_stmt_init(), out of memory" << endl;
		}
		if (mysql_stmt_prepare(pstmt, query_str.str().c_str(), query_str.str().length()))
		{
			cout << __func__<< ": mysql_stmt_prepare(), INSERT failed" << endl;
		}

		/*param_count= mysql_stmt_param_count(pstmt);

		if (param_count != 2)
		{
			cout << __func__<< ": invalid parameter count returned by MySQL" << endl;
		}*/
		memset(plog_bind, 0, sizeof(MYSQL_BIND)*2);

		plog_bind[0].buffer_type= MYSQL_TYPE_DOUBLE;
		if(value_r.size() == 1)
			plog_bind[0].buffer= (void *)&value_r[0];
		plog_bind[0].is_null= &is_null[0];
		plog_bind[0].length= 0;

		plog_bind[1].buffer_type= MYSQL_TYPE_DOUBLE;
		if(value_w.size() == 1)
			plog_bind[1].buffer= (void *)&value_w[0];
		plog_bind[1].is_null= &is_null[1];
		plog_bind[1].length= 0;

		if (mysql_stmt_bind_param(pstmt, plog_bind))
			cout << __func__<< ": mysql_stmt_bind_param() failed" << endl;

		if (mysql_stmt_execute(pstmt))
		{
			cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
			delete [] plog_bind;
			if (mysql_stmt_close(pstmt))
				cout << __func__<< ": failed while closing the statement" << endl;
			return -1;
		}
		else
		{
#ifdef _LIB_DEBUG
			cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		}
		delete [] plog_bind;

	/*	if (paffected_rows != 1)
			DEBUG_STREAM << "log_srvc: invalid affected rows " << endl;*/
		if (mysql_stmt_close(pstmt))
			cout << __func__<< ": failed while closing the statement" << endl;
	}
	return 0;
}

int HdbMySQL::store_string_RO(string attr, vector<string> value, double time, Tango::AttrDataFormat data_format, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	int ID=-1;
	string facility = get_only_tango_host(attr);
	string attr_name = get_only_attr_name(attr);
	find_attr_id(facility, attr_name, ID);
	if(ID == -1)
	{

		cout << __func__<< ": ID not found!" << endl;
		return -1;
	}

	ostringstream query_str;
	char attr_tbl_name[64];
	sprintf(attr_tbl_name,"%s%05d",ATT_TABLE_NAME,ID);
	int timestamp = (int)(time);	//TODO
  //example:
  //INSERT INTO attr_12345
  //	(a/b/c/d, e/f/g/h)


  //TODO: concatenate facility and full_name? ... WHERE CONCAT(ADT_COL_FACILITY, ADT_COL_FULL_NAME) IN (...)
  query_str <<
		"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
			" (" << ATT_COL_TIME << ",";
			if(data_format != Tango::SCALAR)
				query_str << ATT_COL_DIMX << ",";
			query_str << ATT_COL_VALUE_RO << ")" <<
			" VALUES (FROM_UNIXTIME(" << timestamp << "),";
			if(data_format != Tango::SCALAR)
				query_str << value.size() << ",";
			query_str <<std::scientific<<setprecision(16)<<"'";
			for(uint32_t i=0; i<value.size(); i++)
			{
				query_str << value[i];
				if(i != value.size()-1)
					query_str << ", ";
			 }
			query_str << "')";


	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		return -1;
	}
	else
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
	}
	return 0;
}

int HdbMySQL::store_string_RW(string attr, vector<string> value_r, vector<string> value_w, double time, Tango::AttrDataFormat data_format, bool isNull)
{
#ifdef _LIB_DEBUG
//	cout << __func__<< ": entering..." << endl;
#endif
	int ID=-1;
	string facility = get_only_tango_host(attr);
	string attr_name = get_only_attr_name(attr);
	find_attr_id(facility, attr_name, ID);
	if(ID == -1)
	{

		cout << __func__<< ": ID not found!" << endl;
		return -1;
	}

	ostringstream query_str;
	char attr_tbl_name[64];
	sprintf(attr_tbl_name,"%s%05d",ATT_TABLE_NAME,ID);
	int timestamp = (int)(time);	//TODO
  //example:
  //INSERT INTO attr_12345
  //	(a/b/c/d, e/f/g/h)


  //TODO: concatenate facility and full_name? ... WHERE CONCAT(ADT_COL_FACILITY, ADT_COL_FULL_NAME) IN (...)
  query_str <<
		"INSERT INTO " << m_dbname << "." << attr_tbl_name <<
			" (" << ATT_COL_TIME << ",";
			if(data_format != Tango::SCALAR)
				query_str << ATT_COL_DIMX << ",";
			query_str << ATT_COL_R_VALUE_RW << "," << ATT_COL_W_VALUE_RW << ")" <<
			" VALUES (FROM_UNIXTIME(" << timestamp << "),";
			if(data_format != Tango::SCALAR)
				query_str << value_r.size() << ",";		//TODO: max(r,w) size
			query_str <<std::scientific<<setprecision(16)<<"'";
			for(uint32_t i=0; i<value_r.size(); i++)
			{
				query_str << value_r[i];
				if(i != value_r.size()-1)
					query_str << ", ";
			}
			query_str << "','";
			for(uint32_t i=0; i<value_w.size(); i++)
			{
				query_str << value_w[i];
				if(i != value_w.size()-1)
					query_str << ", ";
			}
			query_str << "')";


	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << endl;
		return -1;
	}
	else
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
	}
	return 0;
}

int HdbMySQL::configure_Attr(string name, int type/*DEV_DOUBLE, DEV_STRING, ..*/, int format/*SCALAR, SPECTRUM, ..*/, int write_type/*READ, READ_WRITE, ..*/)
{
	ostringstream insert_str;
	string facility = get_only_tango_host(name);
	string attr_name = get_only_attr_name(name);

	int id=-1;
	int ret = find_attr_id_type(facility, attr_name, id, type, format, write_type);
	//ID already present but different configuration (attribute type)
	if(ret == -2)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" already configured with ID="<<id << endl;
		return -1;
	}

	//ID found and same configuration (attribute type): do nothing
	if(ret == 0)
	{
		cout<< __func__ << ": ALREADY CONFIGURED with same configuration: "<<facility<<"/"<<attr_name<<" with ID="<<id << endl;
		return 0;
	}
	//ID not found: create new table
	insert_str <<
		"INSERT INTO " << m_dbname << "." << ADT_TABLE_NAME << " ("<<ADT_COL_FULL_NAME<<","<<ADT_COL_FACILITY<<","<<ADT_COL_TIME<<","<<
			ADT_COL_DATA_TYPE<<","<<ADT_COL_DATA_FORMAT<<","<<ADT_COL_WRITABLE<<")" <<
			" VALUES ('" << attr_name << "','" << facility << "',NOW(),"<<
				type<<","<<format<<","<<write_type<<")";

	if(mysql_query(dbp, insert_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << insert_str.str() << endl;
		return -1;
	}

	else
	{
		int last_id = mysql_insert_id(dbp);
		char attr_tbl_name[64];
		sprintf(attr_tbl_name,"%s%05d",ATT_TABLE_NAME,last_id);
		ostringstream create_str;

		//TODO: type
		Attr_Type attr_type = get_attr_type(type, format, write_type);
		switch(attr_type)
		{
			case scalar_double_ro:
			{
				create_str <<
					"CREATE TABLE IF NOT EXISTS " << m_dbname << "." << attr_tbl_name <<
						"( " <<
							ATT_COL_TIME << " datetime not null default '0000-00-00 00:00:00'," <<
							ATT_COL_VALUE_RO << " double default NULL," <<
							"INDEX(time)" <<
						")";
			}
			break;
			case scalar_double_rw:
			{
				create_str <<
					"CREATE TABLE IF NOT EXISTS " << m_dbname << "." << attr_tbl_name <<
						"( " <<
							ATT_COL_TIME << " datetime not null default '0000-00-00 00:00:00'," <<
							ATT_COL_R_VALUE_RW << " double default NULL," <<
							ATT_COL_W_VALUE_RW << " double default NULL," <<
							"INDEX(time)" <<
						")";
			}
			break;
			case array_double_ro:
			{
				create_str <<
					"CREATE TABLE IF NOT EXISTS " << m_dbname << "." << attr_tbl_name <<
						"( " <<
							ATT_COL_TIME << " datetime not null default '0000-00-00 00:00:00'," <<
							ATT_COL_DIMX << " smallint(6) not null," <<
							ATT_COL_VALUE_RO << " blob default NULL," <<
							"INDEX(time)" <<
						")";
			}
			break;
			case array_double_rw:
			{
				create_str <<
					"CREATE TABLE IF NOT EXISTS " << m_dbname << "." << attr_tbl_name <<
						"( " <<
							ATT_COL_TIME << " datetime not null default '0000-00-00 00:00:00'," <<
							ATT_COL_DIMX << " smallint(6) not null," <<
							ATT_COL_R_VALUE_RW << " blob default NULL," <<
							ATT_COL_W_VALUE_RW << " blob default NULL," <<
							"INDEX(time)" <<
						")";
			}
			break;
			case scalar_string_ro:
			{
				create_str <<
					"CREATE TABLE IF NOT EXISTS " << m_dbname << "." << attr_tbl_name <<
						"( " <<
							ATT_COL_TIME << " datetime not null default '0000-00-00 00:00:00'," <<
							ATT_COL_VALUE_RO << " varchar(255) default NULL," <<
							"INDEX(time)" <<
						")";
			}
			break;
			default:
				cout << __func__ << ": attribute type not supported: data_type="<< type << " data_format=" << format << " writable=" << write_type << endl;
				return -1;
		}


		if(mysql_query(dbp, create_str.str().c_str()))
		{
			cout<< __func__ << ": ERROR in query=" << create_str.str() << endl;
			return -1;
		}
		return 0;
	}
}

int HdbMySQL::remove_Attr(string name)
{
	//TODO: implement
	return 0;
}

int HdbMySQL::start_Attr(string name)
{
	ostringstream query_str;
	ostringstream update_event_str;
	ostringstream insert_event_str;
	string facility = get_only_tango_host(name);
	string attr_name = get_only_attr_name(name);

	int id=0;
	int ret = find_attr_id(facility, attr_name, id);
	if(ret < 0)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND" << endl;
		return -1;
	}

	query_str <<
		"SELECT " << AMT_COL_ID << " FROM " << m_dbname << "." << AMT_TABLE_NAME <<
			" WHERE " << AMT_COL_ID << "=" <<id;
	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << " err="<<mysql_error(dbp)<< endl;
		return -1;
	}
	else
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{
			insert_event_str <<
				"INSERT INTO " << m_dbname << "." << AMT_TABLE_NAME << " ("<<AMT_COL_ID<<","<<AMT_COL_STARTDATE<<")" <<
					" VALUES ("<<id<<",NOW())";

			if(mysql_query(dbp, insert_event_str.str().c_str()))
			{
				cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
				return -1;
			}
#ifdef _LIB_DEBUG
			else
				cout << __func__<< ": SUCCESS in query: " << insert_event_str.str() << endl;
#endif
			return 0;
		}
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
#ifdef _LIB_DEBUG
			cout << __func__<< ": mysql_num_rows="<< num_found <<" in query: " << query_str.str() << endl;
#endif
			if(num_found == 0)
			{
				insert_event_str <<
					"INSERT INTO " << m_dbname << "." << AMT_TABLE_NAME << " ("<<AMT_COL_ID<<","<<AMT_COL_STARTDATE<<")" <<
						" VALUES ("<<id<<",NOW())";

				if(mysql_query(dbp, insert_event_str.str().c_str()))
				{
					cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
					mysql_free_result(res);
					return -1;
				}
	#ifdef _LIB_DEBUG
				else
					cout << __func__<< ": SUCCESS in query: " << insert_event_str.str() << endl;
	#endif
				mysql_free_result(res);
				return 0;
			}
		}
		mysql_free_result(res);
#if 0	//do nothing if already started once
		update_event_str <<
			"UPDATE " << m_dbname << "." << AMT_TABLE_NAME << " SET "<<AMT_COL_STARTDATE<<"=NOW()" <<
				" WHERE " << AMT_COL_ID << "=" << id;

		if(mysql_query(dbp, update_event_str.str().c_str()))
		{
			cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
			return -1;
		}
#endif
	}

	return 0;
}

int HdbMySQL::stop_Attr(string name)
{
	ostringstream query_str;
	ostringstream update_event_str;
	ostringstream insert_event_str;
	string facility = get_only_tango_host(name);
	string attr_name = get_only_attr_name(name);

	int id=0;
	int ret = find_attr_id(facility, attr_name, id);
	if(ret < 0)
	{
		cout<< __func__ << ": ERROR "<<facility<<"/"<<attr_name<<" NOT FOUND" << endl;
		return -1;
	}

	query_str <<
		"SELECT " << AMT_COL_ID << " FROM " << m_dbname << "." << AMT_TABLE_NAME <<
			" WHERE " << AMT_COL_ID << "=" <<id;

	if(mysql_query(dbp, query_str.str().c_str()))
	{
		cout<< __func__ << ": ERROR in query=" << query_str.str() << " err="<<mysql_error(dbp)<< endl;
		return -1;
	}
	else
	{
#ifdef _LIB_DEBUG
		cout << __func__<< ": SUCCESS in query: " << query_str.str() << endl;
#endif
		MYSQL_RES *res;
		MYSQL_ROW row;
		/*res = mysql_use_result(dbp);
		my_ulonglong num_found = mysql_num_rows(res);
		if(num_found == 0)*/
		res = mysql_store_result(dbp);
		if(res == NULL)
		{

			insert_event_str <<
				"INSERT INTO " << m_dbname << "." << AMT_TABLE_NAME << " ("<<AMT_COL_ID<<","<<AMT_COL_STOPDATE<<")" <<
					" VALUES ("<<id<<",NOW())";

			if(mysql_query(dbp, insert_event_str.str().c_str()))
			{
				cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
				return -1;
			}
#ifdef _LIB_DEBUG
			else
				cout << __func__<< ": SUCCESS in query: " << insert_event_str.str() << endl;
#endif
			return 0;
		}
		else
		{
			my_ulonglong num_found = mysql_num_rows(res);
#ifdef _LIB_DEBUG
			cout << __func__<< ": mysql_num_rows="<< num_found <<" in query: " << query_str.str() << endl;
#endif
			if(num_found == 0)
			{
				insert_event_str <<
					"INSERT INTO " << m_dbname << "." << AMT_TABLE_NAME << " ("<<AMT_COL_ID<<","<<AMT_COL_STOPDATE<<")" <<
						" VALUES ("<<id<<",NOW())";

				if(mysql_query(dbp, insert_event_str.str().c_str()))
				{
					cout<< __func__ << ": ERROR in query=" << insert_event_str.str() << endl;
					mysql_free_result(res);
					return -1;
				}
#ifdef _LIB_DEBUG
				else
					cout << __func__<< ": SUCCESS in query: " << insert_event_str.str() << endl;
#endif
				mysql_free_result(res);
				return 0;
			}
		}
		mysql_free_result(res);

		update_event_str <<
			"UPDATE " << m_dbname << "." << AMT_TABLE_NAME << " SET "<<AMT_COL_STOPDATE<<"=NOW()" <<
				" WHERE " << AMT_COL_ID << "=" << id;

		if(mysql_query(dbp, update_event_str.str().c_str()))
		{
			cout<< __func__ << ": ERROR in query=" << update_event_str.str() << endl;
			return -1;
		}
#ifdef _LIB_DEBUG
		else
			cout << __func__<< ": SUCCESS in query: " << update_event_str.str() << endl;
#endif
	}

	return 0;
}

string HdbMySQL::get_only_attr_name(string str)
{
	string::size_type	start = str.find("tango://");
	if (start == string::npos)
		return str;
	else
	{
		start += 8; //	"tango://" length
		start = str.find('/', start);
		start++;
		string	signame = str.substr(start);
		return signame;
	}
}

//=============================================================================
//=============================================================================
string HdbMySQL::get_only_tango_host(string str)
{
	string::size_type	start = str.find("tango://");
	if (start == string::npos)
	{
		return "unknown";
	}
	else
	{
		start += 8; //	"tango://" length
		string::size_type	end = str.find('/', start);
		string th = str.substr(start, end-start);
		return th;
	}
}

//=============================================================================
//=============================================================================
string HdbMySQL::remove_domain(string str)
{
	string::size_type	end1 = str.find(".");
	if (end1 == string::npos)
	{
		return str;
	}
	else
	{
		string::size_type	start = str.find("tango://");
		if (start == string::npos)
		{
			start = 0;
		}
		else
		{
			start = 8;	//tango:// len
		}
		string::size_type	end2 = str.find(":", start);
		if(end1 > end2)	//'.' not in the tango host part
			return str;
		string th = str.substr(0, end1);
		th += str.substr(end2, str.size()-end2);
		return th;
	}
}

AbstractDB* HdbMySQLFactory::create_db(string host, string user, string password, string dbname, int port)
{
	return new HdbMySQL(host, user, password, dbname, port); 
}

DBFactory *HdbClient::getDBFactory()
{
	HdbMySQLFactory *db_mysql_factory = new HdbMySQLFactory();
	return static_cast<DBFactory*>(db_mysql_factory);//TODO
}

