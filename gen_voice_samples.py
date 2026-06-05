"""Генерит по одному WAV-сэмплу на каждый голос Silero v4_ru в ./samples/.
Запуск: python gen_voice_samples.py
"""
import os
import torch

SPEAKERS = ["aidar", "baya", "kseniya", "xenia", "eugene", "random"]
TEXT = (
    "Привет, я Silero. Это пример моей озвучки: "
    "донат пятьсот рублей, спасибо за поддержку стрима!"
)
SR = 48000
OUT = "samples"
os.makedirs(OUT, exist_ok=True)

print("Загружаю Silero v4_ru...")
model, _ = torch.hub.load(
    repo_or_dir="snakers4/silero-models",
    model="silero_tts",
    language="ru",
    speaker="v4_ru",
    trust_repo=True,
)
model.to(torch.device("cpu"))

for sp in SPEAKERS:
    path = os.path.join(OUT, f"{sp}.wav")
    print(f"  {sp} -> {path}")
    model.save_wav(text=TEXT, speaker=sp, sample_rate=SR, audio_path=path,
                   put_accent=True, put_yo=True)
print("готово")
