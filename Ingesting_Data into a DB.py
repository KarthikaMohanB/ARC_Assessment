#!/usr/bin/env python
# coding: utf-8

# In[ ]:


get_ipython().system('pip install psycopg2')
import psycopg2 as ps
import pandas as pd


# In[ ]:


df = pd.read_csv('youtube_vids_1st_pull.csv', index_col=0) 
df.head()


# In[ ]:


def connect_to_db(host_name, dbname, port, username, password):
    try:
        conn = ps.connect(host=host_name, database=dbname, user=username, password=password, port=port)

    except ps.OperationalError as e:
        raise e
    else:
        print('Connected!')
        return conn

def create_table(curr):
    create_table_command = ("""CREATE TABLE IF NOT EXISTS videos (
                    video_id VARCHAR(255) PRIMARY KEY,
                    video_title TEXT NOT NULL,
                    upload_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    view_count INTEGER NOT NULL,
                    like_count INTEGER NOT NULL,
                    dislike_count INTEGER NOT NULL,
                    comment_count INTEGER NOT NULL
            )""")

    curr.execute(create_table_command)

def insert_into_table(curr, video_id, video_title, upload_date, view_count, like_count, dislike_count, comment_count):
    insert_into_videos = ("""INSERT INTO videos (video_id, video_title, upload_date,
                        view_count, like_count, dislike_count,comment_count)
    VALUES(%s,%s,%s,%s,%s,%s,%s);""")
    row_to_insert = (video_id, video_title, upload_date, view_count, like_count, dislike_count, comment_count)
    curr.execute(insert_into_videos, row_to_insert)


def update_row(curr, video_id, video_title, view_count, like_count, dislike_count, comment_count):
    query = ("""UPDATE videos
            SET video_title = %s,
                view_count = %s,
                like_count = %s,
                dislike_count = %s,
                comment_count = %s
            WHERE video_id = %s;""")
    vars_to_update = (video_title, view_count, like_count, dislike_count, comment_count, video_id)
    curr.execute(query, vars_to_update)


def check_if_video_exists(curr, video_id): 
    query = ("""SELECT video_id FROM VIDEOS WHERE video_id = %s""")

    curr.execute(query, (video_id,))
    return curr.fetchone() is not None



def truncate_table(curr):
    truncate_table = ("""TRUNCATE TABLE videos""")

    curr.execute(truncate_table)


def append_from_df_to_db(curr,df):
    for i, row in df.iterrows():
        insert_into_table(curr, row['video_id'], row['video_title'], row['upload_date'], row['view_count']
                          , row['like_count'], row['dislike_count'], row['comment_count'])


def update_db(curr,df):
    tmp_df = pd.DataFrame(columns=['video_id', 'video_title', 'upload_date', 'view_count',
                                   'like_count', 'dislike_count', 'comment_count'])
    for i, row in df.iterrows():
        if check_if_video_exists(curr, row['video_id']): # If video already exists then we will update
            update_row(curr,row['video_id'],row['video_title'],row['view_count'],row['like_count'],row['dislike_count'],row['comment_count'])
        else: # The video doesn't exists so we will add it to a temp df and append it using append_from_df_to_db
            tmp_df = tmp_df.append(row)

    return tmp_df


# In[ ]:


host_name = 'my-host1234.gcpvm.com'
dbname = 'devdealeranalytics'
port = '4481'
username = 'Karthika.arc' 
password = 'Karthika@26'
conn = None

#establish a connection to db
conn = connect_to_db(host_name, dbname, port, username, password)
curr = conn.cursor()


# In[ ]:


create_table(curr)
new_vid_df = update_db(curr,df)
conn.commit()


# In[ ]:


append_from_df_to_db(curr, new_vid_df)
conn.commit()


# In[ ]:


curr.execute("SELECT * FROM VIDEOS")
print(curr.fetchall())

