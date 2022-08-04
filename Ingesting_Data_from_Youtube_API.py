#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import pandas as pd
import time


# In[ ]:


API_KEY = "AIzaSyDFQ350Si7NLLo_kYZTDKMza_YVatAK7VA"
CHANNEL_ID = "UCAuUUnT6oDeKwE6v1NGQxug"


# In[ ]:


def get_video_details(video_id):

    #collecting view, like, dislike, comment counts
    url_video_stats = "https://www.googleapis.com/youtube/v3/videos?id="+video_id+"&part=statistics&key="+API_KEY
    response_video_stats = requests.get(url_video_stats).json()

    view_count = response_video_stats['items'][0]['statistics']['viewCount']
    like_count = response_video_stats['items'][0]['statistics']['likeCount']
    dislike_count = response_video_stats['items'][0]['statistics']['dislikeCount']
    comment_count = response_video_stats['items'][0]['statistics']['commentCount']

    return view_count, like_count, dislike_count, comment_count


# In[ ]:


def get_videos(df):
    pageToken = ""
    while 1:
        url = "https://www.googleapis.com/youtube/v3/search?key="+API_KEY+"&channelId="+CHANNEL_ID+"&part=snippet,id&order=date&maxResults=10000&"+pageToken

        response = requests.get(url).json()
        time.sleep(1) #give it a second before starting the for loop
        for video in response['items']:
            if video['id']['kind'] == "youtube#video":
                video_id = video['id']['videoId']
                video_title = video['snippet']['title']
                video_title = str(video_title).replace("&amp;","")
                upload_date = video['snippet']['publishedAt']
                upload_date = str(upload_date).split("T")[0]
                view_count, like_count, dislike_count, comment_count = get_video_details(video_id)

                df = df.append({'video_id':video_id,'video_title':video_title,
                                "upload_date":upload_date,"view_count":view_count,
                                "like_count":like_count,"dislike_count":dislike_count,
                                "comment_count":comment_count},ignore_index=True)
        try:
            if response['nextPageToken'] != None: #if none, it means it reached the last page and break out of it
                pageToken = "pageToken=" + response['nextPageToken']

        except:
            break


    return df


# In[ ]:


df2 = pd.DataFrame(columns=["video_id","video_title","upload_date","view_count","like_count","dislike_count","comment_count"]) 

df2 = get_videos(df2)
df2


# In[ ]:


df2.to_csv('youtube_vids_2nd_pull.csv')

