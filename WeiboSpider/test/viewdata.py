# -*- coding: utf-8 -*-

import sys, psycopg2, logging
from psycopg2 import errorcodes

username = 'dbuser'
password = '123'
database = 'weibo'
try:
    connector = psycopg2.connect(
        user=username,
        password=password,
        database=database
    )
    cursor = connector.cursor()
    print('Conneting to database successfully!')
except psycopg2.Error as e:
    sys.exit('Failed to connect database. Returned: {0:s}'.format(errorcodes.lookup(e.pgcode)))


cursor.execute('SELECT * from user_info')
users = cursor.fetchall()
users_count = 0
for user in users:
    users_count+=1
    user_id =user[0]
    user_name = user[1]
    print(user_name)
    cursor.execute("SELECT * from post_info where user_id = '%s'" % user_id)
    posts = cursor.fetchall()
    for post in posts:
        post_id = post[1]
        cursor.execute("SELECT * from text where user_id = '%s' and post_id = '%s'" % (user_id, post_id))
        text = cursor.fetchall()
        if len(text) > 2:
            print(text[2])

print("一共抓取到" + str(users_count) + "users")


cursor.execute('SELECT * from fan')
fans = cursor.fetchall()
fans_count = 0
for fan in fans:
    fans_count+=1
    print(fan)
print("一共抓取到" + str(fans_count) + "fans")

connector.close()
