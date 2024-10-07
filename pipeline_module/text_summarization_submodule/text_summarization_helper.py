import json
import csv
from typing import List, Dict, Any
import warnings
from nltk.translate.bleu_score import sentence_bleu, SmoothingFunction
from transformers import pipeline
from web_server_module.web_server_database import get_status_for_youtube_id, update_status, update_module_output
from ..utils_module.utils import return_video_folder_name, SCENE_SEGMENTED_FILE_CSV, SUMMARIZED_SCENES
from ..utils_module.timeit_decorator import timeit

# Suppress the specific warning
warnings.filterwarnings("ignore", message=".*clean_up_tokenization_spaces.*")

class TextSummarization:
    def __init__(self, video_runner_obj: Dict[str, Any]):
        self.video_runner_obj = video_runner_obj
        self.logger = video_runner_obj.get("logger")
        self.summarizer = pipeline("summarization",
                                   model="facebook/bart-large-cnn",
                                   tokenizer_kwargs={"clean_up_tokenization_spaces": True})

    def calculate_bleu_score(self, data: Dict[str, Any]) -> float:
        method1 = SmoothingFunction().method1
        candidate = data['sentence'].split()
        reference_list = [ref.split() for ref in data['reference']]
        weights = (0.25, 0.25, 0.25, 0.25)  # Equal weights for 1-gram to 4-gram
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
        self.logger.debug(f"Summarizing text of length {len(text)}")
        try:
            summary = self.summarizer(text, max_length=max_length, min_length=min_length, do_sample=False)
            self.logger.debug(f"Summary generated, length: {len(summary[0]['summary_text'])}")
            return summary[0]['summary_text']
        except Exception as e:
            self.logger.error(f"Error in summarize_text: {str(e)}")
            raise

    @timeit
    def generate_text_summary(self) -> bool:
        if get_status_for_youtube_id(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"]) == "done":
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

            if not scenes:
                raise ValueError("No scenes found in the input file")

            self.logger.info(f"Loaded {len(scenes)} scenes for summarization")

            summarized_scenes = []
            for i, scene in enumerate(scenes):
                self.logger.debug(f"Processing scene {i + 1}/{len(scenes)}")
                try:
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
                except Exception as e:
                    self.logger.error(f"Error processing scene {i + 1}: {str(e)}")

            if not summarized_scenes:
                raise ValueError("No scenes were successfully summarized")

            output_file = return_video_folder_name(self.video_runner_obj) + "/" + SUMMARIZED_SCENES
            with open(output_file, 'w') as f:
                json.dump(summarized_scenes, f, indent=2)

            self.logger.info(f"Text summarization completed. Output saved to {output_file}")

            # Save the summarization results to the database
            update_module_output(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"],
                                 'text_summarization', {"summarized_scenes": summarized_scenes})

            update_status(self.video_runner_obj["video_id"], self.video_runner_obj["AI_USER_ID"], "done")
            return True

        except Exception as e:
            self.logger.error(f"Error in text summarization: {str(e)}")
            self.logger.exception("Full traceback:")
            return False