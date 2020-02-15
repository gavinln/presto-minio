from pyhive import presto  # or import hive
from pyhive import hive # or import hive


def get_presto():
    cursor = presto.connect('localhost').cursor()
    cursor.execute('SELECT * FROM minio.default.customer_text limit 5')
    result = cursor.fetchall()
    cursor.close()
    return result


def get_hive():
    cursor = hive.connect('localhost').cursor()
    cursor.execute('SELECT * FROM customer_text limit 5')
    result = cursor.fetchall()
    cursor.close()
    return result


def create_hive_text_table():
    cursor = hive.connect('localhost').cursor()
    sql = '''
    create external table customer_text2(id string, fname string, lname string)
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED as TEXTFILE location 's3a://customer-data-text2/'
    '''
    cursor.execute(sql)
    cursor.close()
    return result


def create_hive_parq_table():
    cursor = hive.connect('localhost').cursor()
    sql = '''
    create external table customer_parq(id string, fname string, lname string)
        STORED AS PARQUET location 's3a://customer-data-parq/'
    '''
    cursor.execute(sql)
    sql = 'insert into customer_parq select * from customer_text'
    cursor.execute(sql)
    sql = 'insert into customer_parq select * from customer_text2'
    cursor.execute(sql)
    cursor.close()



def main():
    # print(get_presto())
    # print(get_hive())
    # create_hive_text_table()
    # create_hive_parq_table()



if __name__ == '__main__':
    main()
