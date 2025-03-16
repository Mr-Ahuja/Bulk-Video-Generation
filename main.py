import sys
import os
import random
import hashlib
import tempfile
import threading
import pandas as pd

from PyQt5.QtCore import Qt, QRunnable, QThreadPool, QObject, pyqtSignal
from PyQt5.QtWidgets import (
    QApplication, QWidget, QPushButton, QLabel, QVBoxLayout, QComboBox,
    QSpinBox, QSlider, QFileDialog, QMessageBox, QProgressDialog
)

# Import video clip classes from MoviePy.
from moviepy.editor import ImageClip, concatenate_videoclips, AudioFileClip, VideoFileClip
from moviepy.audio.fx import audio_loop

# --------------------- Global Disk Cache Setup ---------------------
# Use a dedicated subfolder in the system temporary directory.
DISK_CACHE_DIR = os.path.join(tempfile.gettempdir(), "video_export_cache")
os.makedirs(DISK_CACHE_DIR, exist_ok=True)
# Lock to protect concurrent disk cache operations.
DISK_CACHE_LOCK = threading.Lock()

# --------------------- Worker Signals ---------------------
class WorkerSignals(QObject):
    progress = pyqtSignal(int)  # Emit video index finished.
    error = pyqtSignal(str)     # Emit error message.
    finished = pyqtSignal()     # Emit when a task is done.

# --------------------- Crossfade Helper ---------------------
def crossfade_consecutive_clips(clips, fade_duration=1.0):
    """
    Overlap the last fade_duration seconds of the previous clip with the first fade_duration
    seconds of the next clip. Each clip (except the first) is processed with .crossfadein(fade_duration).
    The clips are concatenated with negative padding equal to fade_duration.
    """
    if not clips:
        return None
    if len(clips) == 1:
        return clips[0]
    new_clips = [clips[0]]
    for clip in clips[1:]:
        new_clips.append(clip.crossfadein(fade_duration))
    final = concatenate_videoclips(new_clips, method="compose", padding=-fade_duration)
    return final

# --------------------- Video Export Task ---------------------
class VideoExportTask(QRunnable):
    def __init__(self, base_dir, files_list, export_params, video_idx):
        """
        base_dir: Directory where main.py is located.
        files_list: List of file paths (filtered by category; may be relative).
        export_params: Dictionary with keys:
          - images_per_video, width, height, per_image_time, fade_duration,
            audio_file, output_folder, crossfade (bool), closing_image (optional)
        video_idx: Integer, the video number (used in output filename).
        """
        super().__init__()
        self.base_dir = base_dir
        self.files_list = files_list
        self.export_params = export_params
        self.video_idx = video_idx
        self.signals = WorkerSignals()

    def run(self):
        try:
            images_per_video = self.export_params["images_per_video"]
            width = self.export_params["width"]
            height = self.export_params["height"]
            per_image_time = self.export_params["per_image_time"]
            fade_duration = self.export_params["fade_duration"]
            audio_file = self.export_params["audio_file"]
            output_folder = self.export_params["output_folder"]
            use_crossfade = self.export_params["crossfade"]
            closing_image = self.export_params.get("closing_image", None)

            # Randomly select files (allow repeats if needed)
            if len(self.files_list) < images_per_video:
                sample_files = random.choices(self.files_list, k=images_per_video)
            else:
                sample_files = random.sample(self.files_list, images_per_video)

            clips = []
            for file_path in sample_files:
                # Convert to absolute path if needed.
                if not os.path.isabs(file_path):
                    file_path = os.path.join(self.base_dir, file_path)
                if not os.path.isfile(file_path):
                    print(f"File not found: {file_path}")
                    continue

                lower_path = file_path.lower()
                # If the file is a video, load it as VideoFileClip.
                if lower_path.endswith((".mp4", ".mov", ".avi")):
                    clip = VideoFileClip(file_path).resize(newsize=(width, height))
                    # Ensure uniform resolution by cropping/padding if needed.
                    if clip.w > width:
                        clip = clip.crop(x_center=clip.w/2, width=width)
                    elif clip.w < width:
                        clip = clip.on_color(size=(width, height), color=(0,0,0), pos=('center', 'center'))
                    clips.append(clip)
                else:
                    # Otherwise assume it's an image.
                    cache_key = f"{file_path}_{width}_{height}"
                    hash_key = hashlib.md5(cache_key.encode('utf-8')).hexdigest()
                    cache_file = os.path.join(DISK_CACHE_DIR, f"{hash_key}.png")
                    with DISK_CACHE_LOCK:
                        if os.path.exists(cache_file):
                            processed_file = cache_file
                        else:
                            clip_proc = ImageClip(file_path).resize(height=height)
                            if clip_proc.w > width:
                                clip_proc = clip_proc.crop(x_center=clip_proc.w/2, width=width)
                            elif clip_proc.w < width:
                                clip_proc = clip_proc.on_color(size=(width, height), color=(0, 0, 0), pos=('center', 'center'))
                            clip_proc.save_frame(cache_file, t=0)
                            processed_file = cache_file
                    clip_final = ImageClip(processed_file).set_duration(per_image_time)
                    clips.append(clip_final)

            # Process closing image (always assumed to be an image) if provided.
            closing_clip = None
            if closing_image and closing_image.strip() != "":
                if not os.path.isabs(closing_image):
                    closing_image = os.path.join(self.base_dir, closing_image)
                if os.path.isfile(closing_image):
                    cache_key_close = f"{closing_image}_{width}_{height}"
                    hash_key_close = hashlib.md5(cache_key_close.encode('utf-8')).hexdigest()
                    cache_file_close = os.path.join(DISK_CACHE_DIR, f"{hash_key_close}.png")
                    with DISK_CACHE_LOCK:
                        if os.path.exists(cache_file_close):
                            processed_close = cache_file_close
                        else:
                            clip_close = ImageClip(closing_image).resize(height=height)
                            if clip_close.w > width:
                                clip_close = clip_close.crop(x_center=clip_close.w/2, width=width)
                            elif clip_close.w < width:
                                clip_close = clip_close.on_color(size=(width, height), color=(0, 0, 0), pos=('center', 'center'))
                            clip_close.save_frame(cache_file_close, t=0)
                            processed_close = cache_file_close
                    closing_clip = ImageClip(processed_close).set_duration(3)  # Fixed 3 sec duration

            if not clips:
                raise ValueError("No valid files to process for video creation.")

            # Build the final clip.
            if use_crossfade:
                main_clip = crossfade_consecutive_clips(clips, fade_duration=fade_duration)
                if closing_clip:
                    final_clip = concatenate_videoclips([main_clip, closing_clip], method="compose")
                else:
                    final_clip = main_clip
            else:
                if closing_clip:
                    clips.append(closing_clip)
                final_clip = concatenate_videoclips(clips, method="compose")

            # Attach audio if available.
            if audio_file and os.path.isfile(audio_file):
                audio_clip = AudioFileClip(audio_file)
                if audio_clip.duration < final_clip.duration:
                    audio_clip = audio_loop(audio_clip, duration=final_clip.duration)
                else:
                    audio_clip = audio_clip.subclip(0, final_clip.duration)
                final_clip = final_clip.set_audio(audio_clip)

            output_path = os.path.join(output_folder, f"video_{self.video_idx}.mp4")
            final_clip.write_videofile(
                output_path,
                fps=30,
                codec="libx264",
                audio_codec="aac",
                preset="ultrafast",
                verbose=False,
                logger=None
            )

            self.signals.progress.emit(self.video_idx)
        except Exception as e:
            self.signals.error.emit(f"Error in video {self.video_idx}: {e}")
        self.signals.finished.emit()

# --------------------- Main UI Class ---------------------
class CSVAudioTool(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("CSV and Audio Loader Tool")
        self.setFixedSize(800, 600)
        self.df = None
        self.threadpool = QThreadPool()
        self.threadpool.setMaxThreadCount(3)
        self.tasks_finished = 0
        self.total_tasks = 0
        self.setupUI()

    def setupUI(self):
        layout = QVBoxLayout()

        # Output Ratio Field
        self.output_ratio_label = QLabel("Output Ratio")
        layout.addWidget(self.output_ratio_label)
        self.output_ratio_combo = QComboBox()
        layout.addWidget(self.output_ratio_combo)
        self.output_ratio_map = {
            "Instagram Feed": {"ratio": "1:1", "resolution": "1080x1080"},
            "Instagram Story": {"ratio": "9:16", "resolution": "1080x1920"},
            "Instagram Reel": {"ratio": "9:16", "resolution": "1080x1920"},
            "YouTube Video": {"ratio": "16:9", "resolution": "1920x1080"},
            "YouTube Shorts": {"ratio": "9:16", "resolution": "1080x1920"},
            "Facebook Feed": {"ratio": "1.91:1", "resolution": "1200x628"},
            "Facebook Story": {"ratio": "9:16", "resolution": "1080x1920"},
            "Facebook Reels": {"ratio": "9:16", "resolution": "1080x1920"},
            "TikTok": {"ratio": "9:16", "resolution": "1080x1920"},
            "Portrait": {"ratio": "9:16", "resolution": "1080x1920"},
            "Landscape": {"ratio": "16:9", "resolution": "1920x1080"},
            "Cover": {"ratio": "1:2.39", "resolution": "800x320"}
        }
        self.output_ratio_combo.addItems(list(self.output_ratio_map.keys()))

        # Load CSV Button
        self.load_csv_btn = QPushButton("Load CSV")
        self.load_csv_btn.clicked.connect(self.load_csv)
        layout.addWidget(self.load_csv_btn)

        # Category Dropdown
        self.category_label = QLabel("Select Category")
        layout.addWidget(self.category_label)
        self.category_combo = QComboBox()
        layout.addWidget(self.category_combo)

        # Number of images per Video
        self.spinbox_label = QLabel("Number of image per Video")
        layout.addWidget(self.spinbox_label)
        self.image_spinbox = QSpinBox()
        self.image_spinbox.setRange(1, 100)
        layout.addWidget(self.image_spinbox)

        # Number of output videos
        self.output_videos_label = QLabel("Number of output videos")
        layout.addWidget(self.output_videos_label)
        self.output_videos_spinbox = QSpinBox()
        self.output_videos_spinbox.setRange(1, 100)
        layout.addWidget(self.output_videos_spinbox)

        # Per Image Time slider
        self.slider_label = QLabel("Per Image Time (sec)")
        layout.addWidget(self.slider_label)
        self.time_slider = QSlider(Qt.Horizontal)
        self.time_slider.setRange(1, 10)
        self.time_slider.valueChanged.connect(self.update_slider_value)
        layout.addWidget(self.time_slider)
        self.slider_value_label = QLabel(f"Current value: {self.time_slider.value()} sec")
        layout.addWidget(self.slider_value_label)

        # Select Audio File
        self.audio_btn = QPushButton("Select Audio")
        self.audio_btn.clicked.connect(self.select_audio)
        layout.addWidget(self.audio_btn)
        self.audio_label = QLabel("No audio selected")
        layout.addWidget(self.audio_label)

        # Select Closing Image Field
        self.closing_image_btn = QPushButton("Select Closing Image")
        self.closing_image_btn.clicked.connect(self.select_closing_image)
        layout.addWidget(self.closing_image_btn)
        self.closing_image_label = QLabel("No closing image selected")
        layout.addWidget(self.closing_image_label)

        # Select Output Folder
        self.output_folder_btn = QPushButton("Select Output Folder")
        self.output_folder_btn.clicked.connect(self.select_output_folder)
        layout.addWidget(self.output_folder_btn)
        self.output_folder_label = QLabel("No folder selected")
        layout.addWidget(self.output_folder_label)

        # Start Export Button
        self.export_btn = QPushButton("Start Export")
        self.export_btn.clicked.connect(self.start_export)
        layout.addWidget(self.export_btn)

        self.setLayout(layout)

    def load_csv(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Open CSV", "", "CSV Files (*.csv)")
        if file_path:
            try:
                df = pd.read_csv(file_path)
                if 'Category' in df.columns and 'File' in df.columns:
                    self.df = df
                    categories = df['Category'].dropna().unique().tolist()
                    self.category_combo.clear()
                    self.category_combo.addItems(categories)
                else:
                    self.show_error("CSV must contain 'Category' and 'File' columns")
            except Exception as e:
                self.show_error(f"Failed to load CSV file:\n{e}")

    def select_audio(self):
        audio_path, _ = QFileDialog.getOpenFileName(self, "Select Audio", "", "Audio Files (*.mp3 *.wav *.ogg)")
        if audio_path:
            self.audio_label.setText(audio_path)

    def select_closing_image(self):
        closing_path, _ = QFileDialog.getOpenFileName(self, "Select Closing Image", "", "Image Files (*.jpg *.png)")
        if closing_path:
            self.closing_image_label.setText(closing_path)

    def select_output_folder(self):
        folder_path = QFileDialog.getExistingDirectory(self, "Select Output Folder")
        if folder_path:
            self.output_folder_label.setText(folder_path)

    def start_export(self):
        if self.df is None or self.df.empty:
            self.show_error("Please load a valid CSV first.")
            return

        output_folder = self.output_folder_label.text()
        if not output_folder or not os.path.isdir(output_folder):
            self.show_error("Please select a valid output folder.")
            return

        output_ratio = self.output_ratio_combo.currentText()
        ratio_details = self.output_ratio_map.get(output_ratio, {})
        res_str = ratio_details.get("resolution", "1920x1080")
        try:
            width, height = map(int, res_str.split('x'))
        except Exception:
            width, height = 1920, 1080

        # Pre-filter files by category.
        category = self.category_combo.currentText()
        files_list = self.df[self.df["Category"] == category]["File"].dropna().tolist()
        if not files_list:
            self.show_error("No files found for the selected category.")
            return

        export_params = {
            "images_per_video": self.image_spinbox.value(),
            "output_videos": self.output_videos_spinbox.value(),
            "width": width,
            "height": height,
            "per_image_time": self.time_slider.value(),
            "fade_duration": 1,    # Must be less than per_image_time for a smooth fade.
            "audio_file": self.audio_label.text() if self.audio_label.text() != "No audio selected" else None,
            "output_folder": output_folder,
            "crossfade": True,     # Enable crossfade transitions.
            "closing_image": self.closing_image_label.text() if self.closing_image_label.text() != "No closing image selected" else None
        }

        self.total_tasks = export_params["output_videos"]
        self.tasks_finished = 0

        self.progress_dialog = QProgressDialog("Exporting videos...", "Cancel", 0, self.total_tasks, self)
        self.progress_dialog.setWindowTitle("Export Progress")
        self.progress_dialog.setWindowModality(Qt.WindowModal)
        self.progress_dialog.setValue(0)
        self.progress_dialog.canceled.connect(self.cancel_export)
        self.progress_dialog.show()

        base_dir = os.path.dirname(os.path.abspath(__file__))

        # Launch a task for each video.
        for video_idx in range(1, export_params["output_videos"] + 1):
            task = VideoExportTask(base_dir, files_list, export_params, video_idx)
            task.signals.progress.connect(self.on_task_progress)
            task.signals.error.connect(self.on_task_error)
            task.signals.finished.connect(self.on_task_finished)
            self.threadpool.start(task)

    def on_task_progress(self, video_idx):
        print(f"Video {video_idx} completed.")

    def on_task_error(self, message):
        self.show_error(message)

    def on_task_finished(self):
        self.tasks_finished += 1
        self.progress_dialog.setValue(self.tasks_finished)
        if self.tasks_finished >= self.total_tasks:
            self.progress_dialog.close()
            QMessageBox.information(self, "Export Completed", "Video export process has completed.")

    def cancel_export(self):
        self.threadpool.clear()
        QMessageBox.information(self, "Cancelled", "Export cancelled.")

    def update_slider_value(self, value):
        self.slider_value_label.setText(f"Current value: {value} sec")

    def show_error(self, message):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText(message)
        msg.setWindowTitle("Error")
        msg.exec_()

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = CSVAudioTool()
    window.show()
    sys.exit(app.exec_())
