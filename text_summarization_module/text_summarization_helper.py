#!/usr/bin/env python
# coding: utf-8

# # Scripts for Text Summarization of a single video using BLEU scores
#
# #### Change/Provide the values of the parameters for video id, API URLs
# #### Run the scripts in the given order

# ### Function for obtaining bleu scores of an input containing a sentence and reference sentences

# In[ ]:


import math
import collections
import csv
import json
import pandas as pd
import requests
import sys
from nltk.translate.bleu_score import sentence_bleu
from utils import return_video_folder_name,SCENE_SEGMENTED_FILE_CSV,SUMMARIZED_SCENES
import os
from timeit_decorator import timeit

from nltk.translate.bleu_score import SmoothingFunction


def calculateBleuScore(data):
    method1 = SmoothingFunction().method1
    sentence = data['sentence']
    reference = data['reference']

    candidate = sentence.split(' ')
#     print('candidate: ', candidate)

    referenceList = []
    for i in range(len(reference)):
        arr = reference[i].split(' ')
        referenceList.append(arr)
#     print('refernceList-> ', referenceList)

    # score = sentence_bleu(referenceList, sentence)

    onegram = sentence_bleu(referenceList, candidate, weights=(1, 0, 0, 0), smoothing_function=method1)
    twogram = sentence_bleu(referenceList, candidate, weights=(0.5, 0.5, 0, 0), smoothing_function=method1)
    threegram = sentence_bleu(referenceList, candidate,
                              weights=(0.33, 0.33, 0.33, 0), smoothing_function=method1)
    fourgram = sentence_bleu(referenceList, candidate,
                             weights=(0.25, 0.25, 0.25, 0.25), smoothing_function=method1)
    avg = (onegram + twogram + threegram + fourgram)/4
#     print(onegram, twogram, threegram, fourgram)
#     print(avg*100)
#     print(score)
    return avg


# ### Algorithm for creating different groups of similar captions

# In[ ]:


# Filtering the extracted keyframes based on BLEU score and scene timestamps

# Group keyfrmes as per scene
# Use BLEU score to drop similar keyframes within the scene
# Challenge: which ones to drop?

@timeit
def text_summarization(video_id):
    URL = "http://127.0.0.1:5001/videoSceneData?videoid=" + video_id
    r = requests.get(url=URL)
    scene_arr = r.text
    scene_arr = json.loads(scene_arr)
    scene_arr.sort(key=lambda x: x['scene_num'])

    URL = "http://127.0.0.1:5001/getRevisedKeyFrames?videoId=" + video_id
    r = requests.get(url=URL)
    kf_arr = r.text
    kf_arr = json.loads(kf_arr)

    for j in range(len(scene_arr)):
        scene_arr[j]['kf_data'] = []

    kf_data = {}
    j = 0
    for i in range(len(kf_arr)):
        while j < len(scene_arr) and scene_arr[j]['start_time'] < kf_arr[i]['timestamp']:
            j += 1
        scene_arr[j-1]['kf_data'].append(kf_arr[i])

    for i in range(len(scene_arr)):
        sc = scene_arr[i]
        dur = sc['end_time'] - sc['start_time']
        kf_num = len(sc['kf_data'])
        if kf_num <= 3:
            kf_data[i] = [[idx] for idx in range(kf_num)]
            continue

        sentences_group = []
        captions, score_dict = [], collections.defaultdict(list)
        for kf in sc['kf_data']:
            captions.append(kf['pythia_caption'])
            # calculating bleu scores for every pair of captions
        for idx, cap in enumerate(captions):
            for j in range(0, len(captions)):
                if j == idx:
                    continue
                score1 = calculateBleuScore(
                    {'sentence': cap, 'reference': [captions[j]]})
                score2 = calculateBleuScore(
                    {'sentence': captions[j], 'reference': [cap]})
                score_dict[idx].append((j, max(score1, score2)))
            # Depth first search approach for forming groups of similar captions
        visited = set()
        for idx, cap in enumerate(captions):
            if idx in visited:
                continue
            stack = [(idx, -1)]
            sentences_group.append([[], []])
            prev_set = visited.copy()
            while stack:
                index, sc = stack.pop()
                if index in visited:
                    continue
                sentences_group[-1][0].append(index)
                sentences_group[-1][1].append(sc)
                visited.add(index)
                for j, score in score_dict[index]:
                    # merge captions in a group if score is above 0.4
                    if score >= 0.4:
                        stack.append((j, score))
            # sorting captions by length followed by sum of bleu scores
        sentences_group.sort(key=lambda x: (-len(x[0]), -sum(x[1])))
        # selecting only upto best three captions
        sentences_group = sentences_group[:3]

        # ### Insertion of the obtained captions in db

    # code/API calls for insertion of summarized text in db

    URL = "http://localhost:5001/keyframe/addTextSummarizedKeyFrame"
    for i, sc in enumerate(scene_arr):
        indices = getBestCaptionList(kf_data[i], sc['kf_data'])
    #     print('res: ',indices,i)

        for idx in indices:
            kf = sc['kf_data'][idx]
            body = {
                "keyframeId": kf['keyframe_id'],
                "videoId": kf['video_id'],
                "keyframeNum": kf['keyframe_num'],
                "keyframeURL": kf['keyframe_url'],
                "timestamp": kf['timestamp'],
                "caption": kf['pythia_caption'],
                "sceneId": kf['scene_id']
            }
    #         print(body)
            r = requests.post(url=URL, data=body)
    #         print(r)

    body = {
        video_id: kf_data
    }
    r = requests.post(
        url="http://localhost:5001/keyframe/updateCaption", data=body)


# ### CSV version of text_summarization() algorithm above

# In[ ]:


def text_summarization_csv(video_runner_obj):

    scene_arr = []
    file = return_video_folder_name(video_runner_obj)+'/'+SCENE_SEGMENTED_FILE_CSV
    # Open the requested CSV file, read it, and append the lines
    # to scene_arr
    with open(file, "r") as f:
        reader = csv.reader(f)

        for i, line in enumerate(reader):
            scene_arr.append(line)
    # end of read CSV file

    # scene_arr Structure:
    # 0 - Start Time
    # 1 - End Time
    # 2 - Description (All keyframe descriptions within scene segment)

    for j in range(len(scene_arr)):
        # Get the scene segment keyframe descriptions
        kf_arr = scene_arr[j][2].split('\n')

        scene_arr[j][2] = [i for i in kf_arr if len(i) > 0 and i != ' ']

    kf_data = {}

    j = 0

    print("Number of scenes: ", len(scene_arr))

    scene_output = []
    sentences = []
    for i in range(len(scene_arr)):

        # Assuming that first row in CSV file is the column headers
        # so ignore this "scene"
        if i == 0:
            continue

        scene = scene_arr[i]

        out = {
            "scene_number": i,
            "start_time": scene[0],
            "end_time": scene[1]
        }

        scene_output.append(out)
        print(
            f"======================Scene #{i}==============================")
        print(f"Start Time: {scene[0]} | End Time: {scene[1]}")

        scene_duration = float(scene[1]) - float(scene[0])

        # print(f"Scene Duration: {scene_duration}")

        kf_num = len(scene[2])

        if kf_num <= 3:
            kf_data[i] = [[idx] for idx in range(kf_num)]
            continue

        sentences_group = []
        captions, score_dict = [], collections.defaultdict(list)

        for kf in scene[2]:
            captions.append(kf)

        for idx, cap in enumerate(captions):
            for j in range(0, len(captions)):
                if j == idx:
                    continue
                score1 = calculateBleuScore(
                    {'sentence': cap, 'reference': [captions[j]]})
                score2 = calculateBleuScore(
                    {'sentence': captions[j], 'reference': [cap]})
                score_dict[idx].append((j, max(score1, score2)))
            # Depth first search approach for forming groups of similar captions
        visited = set()
        for idx, cap in enumerate(captions):
            if idx in visited:
                continue
            stack = [(idx, -1)]
            sentences_group.append([[], []])
            prev_set = visited.copy()
            while stack:
                index, sc = stack.pop()
                if index in visited:
                    continue
                sentences_group[-1][0].append(index)
                sentences_group[-1][1].append(sc)
                visited.add(index)
                for j, score in score_dict[index]:
                    # merge captions in a group if score is above 0.4
                    if score >= 0.4:
                        stack.append((j, score))
            # sorting captions by length followed by sum of bleu scores
        sentences_group.sort(key=lambda x: (-len(x[0]), -sum(x[1])))
        # selecting only upto best three captions
        sentences_group = sentences_group[:3]
        scene_text = []
        # For each group of similar sentences, return the best caption (having highest degree of similarity)
        for idx in range(len(sentences_group)):
            # print("Sentence Group: ", idx)
            # print(sentences_group[idx])

            # sentences_group structure:
            # 0 - List of indices of the captions found in scene_arr[i][2]
            # 1 - List of similarity scores of the captions of the indices in 0
            # len(sentences_group[0]) == len(sentences_group[1])

            index = getBestCaptionListCSV(sentences_group[idx][0], scene[2])

            # print('Best Index: ', index)
            scene_text.append(scene[2][index[0]])
            sentence = {
                "start_time": scene[0],
                "text": scene_text,
                "scene_number": i
            }
            sentences.append(sentence)
            # print(scene[2][index[0]])
            # print('\n\n')
            
    ## Filter Duplicate Scenes
    seen = set()
    filtered_sentences = []
    for sentence in sentences:
        scene_number = sentence['scene_number']
        if scene_number not in seen:
            seen.add(scene_number)
            filtered_sentences.append(sentence)
            
    fileName = return_video_folder_name(video_runner_obj)+'/'+SUMMARIZED_SCENES
    if os.path.exists(fileName):
        os.remove(fileName)
    f = open(fileName, "w+")
    f.write(json.dumps(filtered_sentences))
    f.close()

    # for i, scene in enumerate(scene_arr):
    #     if i == 0: continue # ignore first row, assume header column

    #     indices = getBestCaptionListCSV(sentences_group[i], scene[2])
    #     print('res: ', indices, i)


# ### Function for selection of the best caption (having highest degree of similarity) from a group of similar captions
# In[ ]:

def getBestCaptionList(cap_idx_list, data):
    print(cap_idx_list)
    res = []
    for lst in cap_idx_list:
        captions = []
        for idx in lst:
            captions.append(data[idx]['pythia_caption'])
        best_cap_idx = 0
        best_score = -1
#         print(lst)
        if len(captions) == 1:
            res.append(lst[0])
            continue
        for i, cap in enumerate(captions):
            caps = captions.copy()
            del caps[i]
            score = calculateBleuScore({'sentence': cap, 'reference': caps})
            if score > best_score:
                best_cap_idx = i
                best_score = score
        res.append(lst[best_cap_idx])
    return res


# ### Function for selection of the best caption (having highest degree of similarity) from a group of simlar captions for text_summarization_csv where "data" isn't a dictionary
# In[ ]:

def getBestCaptionListCSV(cap_idx_list, data):
    print(cap_idx_list)
    res = []
    captions = []

    if len(cap_idx_list) <= 1:
        return cap_idx_list

    for idx in cap_idx_list:
        captions.append(data[idx][2])
    best_cap_idx = 0
    best_score = -1
#         print(cap_idx_list)
    if len(captions) == 1:
        res.append(cap_idx_list[0])
    for i, cap in enumerate(captions):
        caps = captions.copy()
        score = calculateBleuScore({'sentence': cap, 'reference': caps})
        if score > best_score:
            best_cap_idx = i
            best_score = score
    res.append(cap_idx_list[best_cap_idx])
    print(res)
    return res


# %%
if __name__ == '__main__':
    text_summarization_csv(sys.argv[1])
