from flask import Flask,jsonify,request
from datetime import datetime
import logging
import mysql.connector


app=Flask(__name__)
#logging.basicConfig(filename='sqlapi.log', encoding='utf-8', level=logging.DEBUG,format='%(asctime)s %(message)s')

@app.route("/sql_postman",methods=['POST'])         # POST method
def sql_postman():
    try:
        if (request.method=='POST'):
            '''JSON Format
            {
                "operation":"create",
                "host":"localhost",
                "user":"root",
                "password":"root",
                "db":"db_name",
                "table":"tb_name"
            }
            '''
            #logging.info("Request: %s",request.json)       #log all the request and response into log file that shown in console
            operation=request.json['operation']             #get operation from request
            host=request.json['host']                       #get host from request
            user=request.json['user']                       #get user from request
            password=request.json['password']               #get password from request
            db=request.json['db']                           #get db from request
            table=request.json['table']                     #get table from request
            ob=mysql.connector.connect(host=host,user=user,password=password,database=db,auth_plugin='mysql_native_password')       #connect to database
            cursor=ob.cursor()                              #create cursor
            
            if (operation=='create'):                       #create table
                '''
                for creating table
                Json Format
                {
                    "columns":{"colums_name":"data_type(size)",..."}
                }
                '''
                col=request.json['columns']                 #get columns from request
                columns=''
                for i in col:
                    columns+=i+' '+col[i]+','               #create columns with data type and size
                columns=columns[:-1]                        #remove last comma
                cursor.execute('create table '+table+'('+columns+')')       #create table
                msg={"status":"success", "msg":"table created"}             #create success message

            elif (operation=='insert'):                     #insert data into table
                '''
                for inserting data
                Json Format
                {
                    'data':"data seprated by comma"
                }
                '''
                data=request.json['data']                   #get data from request
                cursor.execute('insert into '+table+' values'+data+'')      #insert data into table
                msg={"status":"success", "msg":"data inserted"}             #create success message
               
            elif (operation=='update'):                     #update data into table
                '''
                for updating data
                Json Format
                {
                    "set": "key=value" pair of columns & values to be updated
                    "where": "condition"
                }
                '''
                set=request.json['set']                     #get set from request
                where=request.json['where']                 #get where from request
                cursor.execute('update '+table+' set '+set+' where '+where)     #update data into table
                if cursor.rowcount > 0:                                         #check if data updated or if updated that means data is present if not then data is not present
                    msg={"status":"success", "msg":"data updated"}              #create success message
                else:                                                           #if data not updated
                    msg={"status":"failed", "msg":"Unable to updated"}          #create failed message

            elif (operation=='delete'):                     #delete data into table
                '''
                for deleting data
                Json Format
                {
                    "table":"tb_name",
                    "where": "condition"
                }
                '''
            
                where=request.json['where']                 #get where from request
                cursor.execute('delete from '+table+' where '+where)            #delete data into table
                if cursor.rowcount > 0:                                         #check if data deleted or if deleted that means data is present if not then data is not present
                    msg={"status":"success", "msg":"data deleted"}              #create success message
                else:                                                           #if data not deleted
                    msg={"status":"error", "msg":"data not found"}              #create error message
                

            elif (operation=='bulk'):                       #bulk insert data into table
                '''
                for bulk inserting data
                Json Format
                {
                    "f_path":"file_path"",
                    "table":"tb_name",
                    "columns":"columns_name"
                }
                '''
                f_path=request.json['filepath']             #get filepath from request
                col=request.json['columns']                 #get columns from request if columns is not given("") or given asterix ("*"") then it will take all columns
                if (col=='*' or col==''):
                    cursor.execute('load data local infile "'+f_path+'" into table '+table+' fields terminated by \',\' lines terminated by \'\n\' ignore 1 lines ')        #bulk insert data into table
                else:                                       #if columns is given
                    cursor.execute('load data local infile "'+f_path+'" into table '+table+' fields terminated by \',\' lines terminated by \'\n\' ignore 1 lines ('+col+')')       #bulk insert data into table with selected columns of table
                msg={"status":"success", "msg":"data dumped"}          #create success message

            elif (operation=='download'):                   #download data from table
                '''
                for downloading data
                Json Format
                {
                    "table":"tb_name",          
                    "where": "condition"
                }
                '''         
                where=request.json['where']                 #get where from request
                if len(where)!=0:                           #if where is given
                    #outfile path should be given in config file /etc/apparmor.d/usr.sbin.mysqld
                    cursor.execute('select * from '+table+' where '+where+' into outfile \'/home/rohit/sql_api/outfile/'+table+'_'+str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))+'.csv\' fields terminated by \',\' lines terminated by \'\n\'')     #download data from table with where condition
                else:                                       #if where is not given
                    cursor.execute('select * from '+table+' into outfile \'/home/rohit/sql_api/outfile/'+table+'_'+str(datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p"))+'.csv\' fields terminated by \',\' lines terminated by \'\n\'')     #download data from table without where condition i.e all data
                msg={"status":"success", "msg":"data downloaded"}       #create success message

            else:                                           #if operation is not given
                msg={"status":"fail", "msg":"Invalid Operation"}        #create fail message
            ob.commit()                                     #commit changes
            return jsonify(msg)                             #return success message in json format
    except Exception as e:                                  #if any exception occurs
        #logging.error("Exception: %s",e)                   #log error
        return jsonify({"status":"fail", "errors":"Exception -"+str(e)})        #return error message in json format with exception

if __name__=="__main__":                                    #main function
    app.run()                                               #run app
