from vicr_scoring import get_vicr_score_from_service

class VICRService:
    def __init__(self, vicr_service):
        self.vicr_service = vicr_service
    
    def getVICRScore(self):
        get_vicr_score_from_service(self.video_id)
