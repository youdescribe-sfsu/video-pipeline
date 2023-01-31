#!/usr/bin/python
# -*- coding: utf-8 -*-

import shutil
from generate_YDX_captions.data_upload import generateYDXCaption, upload_data
from utils import returnVideoFramesFolder


class GenerateYDXCaptions:

    def __init__(self, video_id):
        self.video_id = video_id

    def uploadAndGenerateCaptions(self):
        upload_data(self.video_id)
        # generateYDXCaption(self.video_id)

    def cleanUpData(self):
        shutil.rmtree(returnVideoFramesFolder(self.video_id))
