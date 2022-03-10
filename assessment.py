import random
import mysql.connector as connection
import pandas as pd
import numpy as np

'''for ignoring userwarning 'pandas only support SQLAlchemy connectable(engine/connection)
ordatabase string URI or sqlite3 DBAPI2 connectionother DBAPI2 objects are not tested, please consider using SQLAlchemy'''
import warnings
warnings.simplefilter("ignore", UserWarning)


mydb = connection.connect(host="localhost", database = 'myhomes',user="root", use_pure=True)
def tables(query):
    table_df = pd.read_sql(query,mydb)
    return table_df

# EXTRACT

properties_df = tables("select * from property;")
representatives_df = tables("SELECT r.rep_id,r.supervisor_id,e.* from employee e RIGHT JOIN representative r ON r.emp_id = e.emp_id;")
customer_df = pd.read_csv('customer.csv') #csv exported from mysql
address_df = tables('select a.*,p.street,p.city,p.country from address a LEFT JOIN postcode p ON p.pcode = a.pcode;')
areas_df = tables('select * from sales_area;')
chief_df = tables('''select c.chief_id,r.rep_id from chief_salesperson c 
inner join employee e on e.emp_id=c.emp_id
inner join representative r on r.emp_id=e.emp_id''')
sales_df = tables('''select p.property_id,p.rep_id,p.buyer_id,a.area_id,p.sold_date,p.price*0.01,p.price+p.price*0.01 from property p
inner join address a on a.address_id=p.address_id
where buyer_id is NOT null;''')

# TRANSFORM

properties_df = properties_df.drop(columns=['seller_id','buyer_id','rep_id','sold_date'])
properties_df = properties_df.rename(columns={'type_id':'type','status_id':'status'})

representatives_df.pop('emp_id')

customer_df.pop('rep_id')

address_df.pop('area_id')

payments = [random.choice(['cash','installment','checks','bank transfer']) for i in range(len(sales_df))]
references = [random.choice(['Y','N']) for i in range(len(sales_df))]
sales_df['reference'] = references
sales_df['payment'] = payments
sales_df = sales_df.rename(columns={'p.price*0.01':'rep_bonus','p.price+p.price*0.01':'total_payment'})
sales_df.index = np.arange(1, len(sales_df)+1)
sales_df = sales_df.reset_index().rename({'index':'sales_id'}, axis = 'columns')
# print(sales_df)

# Handling missing data ~ property and representative tables have missing values

# print(properties_df.isna())
properties_df["built_year"].fillna('2012-12-31', inplace = True)
# print(properties_df)

# print(representatives_df.isna())

#representatives that haven't got supervisor_ids are supervisors so their supervisor_id will be their representative id.
representatives_df['supervisor_id'] = representatives_df.apply(
    lambda row: int(row['rep_id']) if np.isnan(row['supervisor_id']) else row['supervisor_id'],axis=1)

#converting supervisor_ids to integer
representatives_df['supervisor_id'] = representatives_df['supervisor_id'].astype(int)


#updating status and type with their actual values ~  I updated from mysql because of the fk constraint

# properties_df['status'].replace(3,'Sold',inplace=True) 
# properties_df['type'].replace(1,'house',inplace =True)
# properties_df['type'].replace(2,'flat/apartment',inplace =True)
# properties_df['type'].replace(3,'bungalow',inplace =True)
# properties_df['type'].replace(4,'land',inplace =True)
# properties_df['type'].replace(5,'commercial property',inplace =True)


# LOAD

def insert_query(query,value_df):
    with mydb.cursor() as cursor:
        cursor.executemany(query,value_df.values.tolist())
        mydb.commit()
        mydb.close() 


def insert_fk(query,values):
    with mydb.cursor() as cursor:
        # cursor.executemany(query,values)
        cursor.executemany(query,[(value[0],e+1) for e, value in enumerate(values)])  #e+1 rep_ids from 1 to 11
        mydb.commit()
        mydb.close() 


#representative_dim load

insert_rep_query = '''
INSERT INTO representative_dim(
rep_id,first_name,last_name,
email,hire_date,dob,gender)
VALUES (%s, %s, %s, %s, %s, %s, %s)
'''
representatives_copy = representatives_df.copy()
representatives_copy.pop('supervisor_id')
supers = pd.DataFrame(representatives_df['supervisor_id']).values.tolist()
# insert_query(insert_rep_query,representatives_copy)

insert_sup_query = '''
UPDATE representative_dim SET supervisor_id = %s
WHERE rep_id = %s
'''
# insert_fk(insert_sup_query,supers)

#sales_area_dim load
insert_area='''
insert into sales_area_dim(area_id,area_name)
values(%s, %s)
'''
# insert_query(insert_area,areas_df)

#customer_dim load
insert_customer = '''
insert into customer_dim
values(%s,%s,%s,%s)
'''
# insert_query(insert_customer,customer_df)

#address_dim load

insert_address ='''
insert into address_dim
values(%s,%s,%s,%s,%s,%s)
'''
# insert_query(insert_address,address_df)


#property_dim load
insert_prop = '''
insert into property_dim(property_id,status,type,address_id,num_bedrooms,
num_bathrooms,built_year,health_safety,garage,garden,price)
values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
# insert_query(insert_prop,properties_df)


#chief_dim load

insert_chief='''
insert into chief_dim
values(%s,%s)
'''
# insert_query(insert_chief,chief_df)

#sales_fact load
insert_fact='''
insert into sales_fact
values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
'''
# insert_query(insert_fact,sales_df)




# properties_df.to_csv('properties.csv',index=False)
# # print(properties_df)
# representatives_df.to_csv('representatives.csv', index=False) 
# # print(representatives_df)
# customer_df.to_csv('customers.csv',index=False)
# # print(customer_df)
# address_df.to_csv('address.csv',index=False)
# # print(address_df)
# areas_df.to_csv('areas.csv',index=False)
# # print(areas_df)
# chief_df.to_csv('chief.csv',index=False)
# # print(chief_df)
# sales_df.to_csv('sales_fact.csv',index=False)