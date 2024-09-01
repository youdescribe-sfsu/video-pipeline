import json
import csv
from typing import List, Dict, Any
from nltk.translate.bleu_score import sentence_bleu
from nltk.translate.bleu_score import SmoothingFunction
from ..utils_module.utils import (
    read_value_from_file,
    save_value_to_file,
    return_video_folder_name,
    SCENE_SEGMENTED_FILE_CSV,
    SUMMARIZED_SCENES
)
from ..utils_module.timeit_decorator import timeit
from transformers import pipeline

class TextSummarization:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.summarizer = pipeline("summarization", model="facebook/bart-large-cnn")

    def calculate_bleu_score(self, data: Dict[str, Any]) -> float:
        method1 = SmoothingFunction().method1
        sentence = data['sentence']
        reference = data['reference']

        candidate = sentence.split()
        reference_list = [ref.split() for ref in reference]

        weights = (0.25, 0.25, 0.25, 0.25)  # equal weights for 1-gram to 4-gram
        return sentence_bleu(reference_list, candidate, weights=weights, smoothing_function=method1)

    def group_similar_sentences(self, sentences: List[str], threshold: float = 0.4) -> List[List[int]]:
        sentence_groups = []
        visited = set()

        for idx, sentence in enumerate(sentences):
            if idx in visited:
                continue

            group = [idx]
            visited.add(idx)

            for j in range(idx + 1, len(sentences)):
                if j in visited:
                    continue

                score = self.calculate_bleu_score({'sentence': sentence, 'reference': [sentences[j]]})
                if score >= threshold:
                    group.append(j)
                    visited.add(j)

            sentence_groups.append(group)

        return sentence_groups

    def select_best_sentence(self, sentences: List[str], group: List[int]) -> str:
        if len(group) == 1:
            return sentences[group[0]]

        best_score = -1
        best_sentence = ''

        for idx in group:
            others = [sentences[i] for i in group if i != idx]
            score = self.calculate_bleu_score({'sentence': sentences[idx], 'reference': others})
            if score > best_score:
                best_score = score
                best_sentence = sentences[idx]

        return best_sentence

    def summarize_text(self, text: str, max_length: int = 130, min_length: int = 30) -> str:
        summary = self.summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)
        return summary[0]['summary_text']

    @timeit
    def generate_text_summary(self) -> bool:
        if read_value_from_file(video_runner_obj=self.video_runner_obj,
                                key="['TextSummarization']['started']") == 'done':
            self.logger.info("Text summarization already processed")
            return True

        try:
            self.logger.info("Starting text summarization")
            scene_file = return_video_folder_name(self.video_runner_obj) + "/" + SCENE_SEGMENTED_FILE_CSV

            scenes = []
            with open(scene_file, 'r', newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    scenes.append({
                        'start_time': float(row['start_time']),
                        'end_time': float(row['end_time']),
                        'description': row['description']
                    })

            summarized_scenes = []
            for scene in scenes:
                sentences = scene['description'].split('\n')
                groups = self.group_similar_sentences(sentences)

                summarized_description = []
                for group in groups:
                    best_sentence = self.select_best_sentence(sentences, group)
                    summarized_description.append(best_sentence)

                full_description = ' '.join(summarized_description)
                summarized_text = self.summarize_text(full_description)

                summarized_scenes.append({
                    'start_time': scene['start_time'],
                    'end_time': scene['end_time'],
                    'text': summarized_text
                })

            output_file = return_video_folder_name(self.video_runner_obj) + "/" + SUMMARIZED_SCENES
            with open(output_file, 'w') as f:
                json.dump(summarized_scenes, f, indent=2)

            self.logger.info(f"Text summarization completed. Output saved to {output_file}")
            save_value_to_file(video_runner_obj=self.video_runner_obj, key="['TextSummarization']['started']",
                               value='done')
            return True

        except Exception as e:
            self.logger.error(f"Error in text summarization: {str(e)}")
            return False

if __name__ == "__main__":
    # For testing purposes
    video_runner_obj = {
        "video_id": "test_video",
        "logger": print  # Use print as a simple logger for testing
    }
    text_summarization = TextSummarization(video_runner_obj)
    success = text_summarization.generate_text_summary()
    print(f"Text summarization {'succeeded' if success else 'failed'}")