from sqlalchemy import create_engine

from ERPtoCRM.config import conn1,conn

new_account = 'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(conn1['DB_USER'], conn1['DB_PASS'],
                                                                   conn1['DB_HOST'],
                                                                   conn1['DB_PORT'], conn1['DATABASE'])
dms_conn = 'mssql+pymssql://{}:{}@{}:{}/{}?charset=utf8'.format(conn['DB_USER'], conn['DB_PASS'],
                                                                conn['DB_HOST'],
                                                                conn['DB_PORT'], conn['DATABASE'])
dms_engine = create_engine(dms_conn)
new_engine = create_engine(new_account)


