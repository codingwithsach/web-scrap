import os
import re
import requests
import logging
import time
import sqlite3
import threading
import csv
import datetime
from bs4 import BeautifulSoup
from urllib.parse import urljoin, unquote, urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import mimetypes
import hashlib
import pdfkit
from PIL import Image
import io
import base64
import shutil
import signal
import sys
import json
from gtts import gTTS
import pygame
from pydub import AudioSegment

# Configuration with environment variables
CONFIG = {
    'BASE_URL': "https://en.wikipedia.org",
    'ROOT_DIR': os.getenv('ARCHIVER_DATA_PATH', '/data/current_events'),
    'MAX_WORKERS': int(os.getenv('MAX_WORKERS', 1)),
    'REQUEST_DELAY': float(os.getenv('REQUEST_DELAY', 5)),
    'MAX_ARTICLES': int(os.getenv('MAX_ARTICLES', 1000)),
    'DAYS_TO_COVER': 1,
    'MEDIA_TYPES': {
        'images': ['jpg', 'jpeg', 'png', 'gif', 'svg', 'webp', 'bmp', 'ico'],
        'videos': ['mp4', 'webm', 'ogg', 'ogv', 'avi', 'mov', 'wmv', 'flv', 'mkv'] if os.getenv('ENABLE_VIDEO', 'true').lower() == 'true' else [],
        'audio': ['mp3', 'ogg', 'wav', 'aac', 'flac', 'm4a', 'wma', 'opus'] if os.getenv('ENABLE_AUDIO', 'true').lower() == 'true' else []
    },
    'MAX_MEDIA_SIZE': int(os.getenv('MAX_MEDIA_SIZE_MB', 100)) * 1024 * 1024,
    'TIMEOUT': 30,
    'WKHTMLTOPDF_PATHS': [
        os.getenv('WKHTMLTOPDF_PATH', '/usr/local/bin/wkhtmltopdf'),
        '/usr/local/bin/wkhtmltopdf',
        '/usr/bin/wkhtmltopdf',
        'wkhtmltopdf'
    ],
    'PDF_OPTIONS': {
        'encoding': 'UTF-8',
        'quiet': '',
        'page-size': 'A4',
        'margin-top': '15mm',
        'margin-right': '15mm',
        'margin-bottom': '15mm',
        'margin-left': '15mm',
        'enable-local-file-access': '',
        'no-outline': None,
        'print-media-type': None,
        'dpi': 96,
        'image-quality': 180,
        'footer-center': '[page]',
        'footer-font-size': '8',
        'header-left': 'Wikipedia Archive',
        'header-font-size': '8',
        'header-spacing': '5',
    },
    'TTS_ENABLED': os.getenv('ENABLE_TTS', 'true').lower() == 'true',
    'TTS_LANGUAGE': os.getenv('TTS_LANGUAGE', 'en'),
    'TTS_SPEED': float(os.getenv('TTS_SPEED', 1.0)),
    'TTS_CHUNK_SIZE': int(os.getenv('TTS_CHUNK_SIZE', 2000)),
    'TTS_MAX_LENGTH': int(os.getenv('TTS_MAX_LENGTH', 50000)),
    'TTS_AUDIO_FORMAT': os.getenv('TTS_AUDIO_FORMAT', 'mp3'),
    'TTS_BITRATE': os.getenv('TTS_BITRATE', '64k'),
    'SAVE_TEXT_FILE': os.getenv('SAVE_TEXT_FILE', 'true').lower() == 'true',
    'TEXT_FORMAT': os.getenv('TEXT_FORMAT', 'both'),
    'MAX_REQUESTS_PER_MINUTE': int(os.getenv('MAX_REQUESTS_PER_MINUTE', 20)),
    'MAX_MEDIA_PER_ARTICLE': int(os.getenv('MAX_MEDIA_PER_ARTICLE', 50)),
    'USE_THUMBNAILS': os.getenv('USE_THUMBNAILS', 'true').lower() == 'true',
    'THUMBNAIL_WIDTH': int(os.getenv('THUMBNAIL_WIDTH', 800)),
    'BACKOFF_MULTIPLIER': int(os.getenv('BACKOFF_MULTIPLIER', 2)),
    'MAX_BACKOFF_SECONDS': int(os.getenv('MAX_BACKOFF_SECONDS', 300)),
    'SECTION_TTS': os.getenv('SECTION_TTS', 'true').lower() == 'true',
    'EXTRACT_REFERENCES': os.getenv('EXTRACT_REFERENCES', 'true').lower() == 'true',
    'DOWNLOAD_MEDIA': os.getenv('DOWNLOAD_MEDIA', 'true').lower() == 'true',
}

class WikipediaArchiver:
    def __init__(self):
        self.shutdown_flag = False
        self.setup_signal_handlers()
        self.setup_logging()
        self.validate_environment()
        self.setup_directories()
        self._thread_connections = {}
        self.db_conn = self.get_db_connection()
        self.initialize_database()
        self.pdfkit_config = self.configure_pdfkit()
        self.setup_pdf_styles()
        
        # Enhanced rate limiting
        self.request_semaphore = threading.Semaphore(CONFIG['MAX_WORKERS'])
        self.last_request_time = 0
        self.domain_last_request = {}
        self.domain_failures = {}
        self.request_counter = 0
        self.minute_start = time.time()
        self.tts_lock = threading.Lock()
        
        self.user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'

    def setup_signal_handlers(self):
        """Setup graceful shutdown handlers"""
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Received shutdown signal {signum}, cleaning up...")
        self.shutdown_flag = True

    def validate_environment(self):
        """Validate environment and dependencies"""
        required_dirs = [CONFIG['ROOT_DIR']]
        for media_type in CONFIG['MEDIA_TYPES'].keys():
            required_dirs.append(os.path.join(CONFIG['ROOT_DIR'], 'media', media_type))
        
        for directory in required_dirs:
            try:
                os.makedirs(directory, exist_ok=True)
                test_file = os.path.join(directory, '.write_test')
                with open(test_file, 'w') as f:
                    f.write('test')
                os.remove(test_file)
                logging.info(f"Directory validated: {directory}")
            except Exception as e:
                logging.error(f"Directory validation failed for {directory}: {e}")
                raise
        
        stat = shutil.disk_usage(CONFIG['ROOT_DIR'])
        free_gb = stat.free / (1024 ** 3)
        if free_gb < 1:
            logging.warning(f"Low disk space: {free_gb:.2f}GB free")

    def setup_logging(self):
        """Configure logging"""
        log_dir = os.path.join(CONFIG['ROOT_DIR'], 'logs')
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"archive_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s',
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler(),
            ]
        )
        logging.info("Wikipedia Archiver started")

    def setup_directories(self):
        """Create required directory structure"""
        os.makedirs(CONFIG['ROOT_DIR'], exist_ok=True)
        for media_type in CONFIG['MEDIA_TYPES'].keys():
            os.makedirs(os.path.join(CONFIG['ROOT_DIR'], 'media', media_type), exist_ok=True)
        
        temp_dir = os.path.join(CONFIG['ROOT_DIR'], 'temp')
        os.makedirs(temp_dir, exist_ok=True)

    def get_db_connection(self):
        """Get thread-safe database connection"""
        thread_id = threading.get_ident()
        
        if thread_id not in self._thread_connections:
            conn = sqlite3.connect(
                os.path.join(CONFIG['ROOT_DIR'], 'wikipedia.db'),
                timeout=30,
                check_same_thread=False
            )
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA foreign_keys=ON")
            self._thread_connections[thread_id] = conn
        
        return self._thread_connections[thread_id]

    def close_db_connections(self):
        """Close all database connections"""
        for conn in self._thread_connections.values():
            try:
                conn.close()
            except Exception as e:
                logging.error(f"Error closing database connection: {e}")
        self._thread_connections.clear()

    def initialize_database(self):
        """Initialize database with enhanced media support"""
        cursor = self.db_conn.cursor()
        
        cursor.executescript('''
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT UNIQUE,
            url TEXT UNIQUE,
            summary TEXT,
            last_updated TEXT,
            article_dir TEXT,
            html_path TEXT,
            pdf_path TEXT,
            tts_path TEXT,
            text_path TEXT,
            formatted_text_path TEXT,
            sections_text_path TEXT,
            references_text_path TEXT,
            scrape_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            status TEXT DEFAULT 'completed',
            total_sections INTEGER DEFAULT 0,
            total_paragraphs INTEGER DEFAULT 0,
            total_references INTEGER DEFAULT 0,
            total_images INTEGER DEFAULT 0,
            total_videos INTEGER DEFAULT 0,
            total_audio INTEGER DEFAULT 0,
            total_media_size INTEGER DEFAULT 0
        );
        
        CREATE TABLE IF NOT EXISTS article_sections (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            section_number INTEGER,
            section_title TEXT,
            section_level INTEGER,
            text_content TEXT,
            formatted_content TEXT,
            text_file_path TEXT,
            tts_file_path TEXT,
            word_count INTEGER,
            char_count INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS article_paragraphs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            section_id INTEGER,
            paragraph_number INTEGER,
            paragraph_text TEXT,
            word_count INTEGER,
            char_count INTEGER,
            reference_ids TEXT,
            media_ids TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (section_id) REFERENCES article_sections (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS article_references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            ref_number INTEGER,
            ref_text TEXT,
            ref_url TEXT,
            ref_title TEXT,
            ref_author TEXT,
            ref_publication TEXT,
            ref_date TEXT,
            ref_access_date TEXT,
            isbn TEXT,
            doi TEXT,
            raw_citation TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS paragraph_references (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paragraph_id INTEGER,
            reference_id INTEGER,
            ref_marker TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paragraph_id) REFERENCES article_paragraphs (id) ON DELETE CASCADE,
            FOREIGN KEY (reference_id) REFERENCES article_references (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS article_tables (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            table_number INTEGER,
            html_path TEXT,
            csv_path TEXT,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            media_type TEXT CHECK(media_type IN ('image', 'video', 'audio')),
            url TEXT UNIQUE,
            original_url TEXT,
            thumbnail_url TEXT,
            local_path TEXT,
            filename TEXT,
            file_extension TEXT,
            file_size INTEGER,
            file_hash TEXT,
            width INTEGER,
            height INTEGER,
            duration REAL,
            title TEXT,
            alt_text TEXT,
            caption TEXT,
            description TEXT,
            license_info TEXT,
            artist TEXT,
            credit TEXT,
            source TEXT,
            download_timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            download_status TEXT DEFAULT 'pending',
            download_attempts INTEGER DEFAULT 0,
            last_attempt DATETIME,
            error_message TEXT,
            metadata TEXT,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS paragraph_media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            paragraph_id INTEGER,
            media_id INTEGER,
            position INTEGER,
            alignment TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (paragraph_id) REFERENCES article_paragraphs (id) ON DELETE CASCADE,
            FOREIGN KEY (media_id) REFERENCES media_files (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS media_download_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            media_id INTEGER,
            priority INTEGER DEFAULT 5,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            last_attempt DATETIME,
            next_attempt DATETIME,
            error_message TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (media_id) REFERENCES media_files (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS archive_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE UNIQUE,
            articles_archived INTEGER DEFAULT 0,
            images_downloaded INTEGER DEFAULT 0,
            videos_downloaded INTEGER DEFAULT 0,
            audio_downloaded INTEGER DEFAULT 0,
            tables_saved INTEGER DEFAULT 0,
            total_size_bytes INTEGER DEFAULT 0,
            total_sections INTEGER DEFAULT 0,
            total_paragraphs INTEGER DEFAULT 0,
            total_references INTEGER DEFAULT 0,
            tts_files_generated INTEGER DEFAULT 0
        );
        
        CREATE TABLE IF NOT EXISTS tts_segments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_id INTEGER,
            section_id INTEGER,
            segment_number INTEGER,
            text_content TEXT,
            audio_path TEXT,
            duration_seconds REAL,
            file_size INTEGER,
            FOREIGN KEY (article_id) REFERENCES articles (id) ON DELETE CASCADE,
            FOREIGN KEY (section_id) REFERENCES article_sections (id) ON DELETE CASCADE
        );
        
        CREATE TABLE IF NOT EXISTS rate_limit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            domain TEXT,
            retry_after INTEGER,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        );
        ''')
        
        # Check and add new columns if they don't exist
        try:
            cursor.execute("SELECT download_status FROM media_files LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE media_files ADD COLUMN download_status TEXT DEFAULT "pending"')
            
        try:
            cursor.execute("SELECT download_attempts FROM media_files LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE media_files ADD COLUMN download_attempts INTEGER DEFAULT 0')
        
        try:
            cursor.execute("SELECT last_attempt FROM media_files LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE media_files ADD COLUMN last_attempt DATETIME')
        
        try:
            cursor.execute("SELECT error_message FROM media_files LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE media_files ADD COLUMN error_message TEXT')
        
        try:
            cursor.execute("SELECT metadata FROM media_files LIMIT 1")
        except sqlite3.OperationalError:
            cursor.execute('ALTER TABLE media_files ADD COLUMN metadata TEXT')
        
        # Create indexes
        cursor.executescript('''
        CREATE INDEX IF NOT EXISTS idx_media_url ON media_files(url);
        CREATE INDEX IF NOT EXISTS idx_media_hash ON media_files(file_hash);
        CREATE INDEX IF NOT EXISTS idx_media_type ON media_files(media_type);
        CREATE INDEX IF NOT EXISTS idx_media_status ON media_files(download_status);
        CREATE INDEX IF NOT EXISTS idx_media_article ON media_files(article_id);
        CREATE INDEX IF NOT EXISTS idx_articles_url ON articles(url);
        CREATE INDEX IF NOT EXISTS idx_articles_timestamp ON articles(scrape_timestamp);
        CREATE INDEX IF NOT EXISTS idx_tts_article ON tts_segments(article_id);
        CREATE INDEX IF NOT EXISTS idx_sections_article ON article_sections(article_id);
        CREATE INDEX IF NOT EXISTS idx_paragraphs_article ON article_paragraphs(article_id);
        CREATE INDEX IF NOT EXISTS idx_paragraphs_section ON article_paragraphs(section_id);
        CREATE INDEX IF NOT EXISTS idx_references_article ON article_references(article_id);
        CREATE INDEX IF NOT EXISTS idx_paragraph_ref_paragraph ON paragraph_references(paragraph_id);
        CREATE INDEX IF NOT EXISTS idx_paragraph_ref_reference ON paragraph_references(reference_id);
        CREATE INDEX IF NOT EXISTS idx_paragraph_media_paragraph ON paragraph_media(paragraph_id);
        CREATE INDEX IF NOT EXISTS idx_paragraph_media_media ON paragraph_media(media_id);
        CREATE INDEX IF NOT EXISTS idx_download_queue_status ON media_download_queue(status);
        CREATE INDEX IF NOT EXISTS idx_download_queue_next ON media_download_queue(next_attempt);
        ''')
        
        self.db_conn.commit()
        logging.info("Database initialized/migrated successfully")

    def setup_pdf_styles(self):
        """Define CSS styles for PDF generation"""
        self.pdf_styles = """
        <style>
            body {
                font-family: 'Arial', sans-serif;
                line-height: 1.6;
                color: #222;
                font-size: 11pt;
                margin: 0;
                padding: 0;
            }
            h1 {
                font-size: 18pt;
                color: #000;
                border-bottom: 2px solid #aaa;
                padding-bottom: 5px;
                margin-top: 10px;
                margin-bottom: 15px;
            }
            h2 {
                font-size: 16pt;
                color: #222;
                border-bottom: 1px solid #eee;
                margin-top: 20px;
                margin-bottom: 10px;
                padding-bottom: 3px;
            }
            h3 {
                font-size: 14pt;
                color: #333;
                margin-top: 15px;
                margin-bottom: 8px;
            }
            p {
                margin: 10px 0;
                text-align: justify;
            }
            a {
                color: #0645ad;
                text-decoration: none;
            }
            a:hover {
                text-decoration: underline;
            }
            img {
                max-width: 100%;
                height: auto;
                margin: 8px 0;
                border: 1px solid #ddd;
                padding: 2px;
            }
            .media-container {
                text-align: center;
                margin: 15px 0;
                padding: 10px;
                background-color: #f9f9f9;
                border: 1px solid #ddd;
            }
            .media-caption {
                font-size: 9pt;
                color: #666;
                margin-top: 5px;
            }
            table {
                border-collapse: collapse;
                width: 100%;
                margin: 12px 0;
                font-size: 10pt;
                border: 1px solid #ddd;
            }
            th, td {
                border: 1px solid #ddd;
                padding: 8px;
                text-align: left;
            }
            th {
                background-color: #f2f2f2;
                font-weight: bold;
            }
            .infobox {
                float: right;
                margin: 0 0 15px 20px;
                border: 1px solid #aaa;
                background-color: #f9f9f9;
                padding: 10px;
                font-size: 10pt;
                width: 280px;
                max-width: 30%;
            }
            .toc {
                display: inline-block;
                border: 1px solid #aaa;
                background-color: #f9f9f9;
                padding: 12px;
                margin: 15px 0;
                font-size: 10pt;
                width: auto;
            }
            .mw-heading {
                border-bottom: 1px solid #aaa;
                padding-bottom: 3px;
            }
            .references {
                font-size: 9pt;
                border-top: 1px solid #eee;
                padding-top: 10px;
                margin-top: 20px;
            }
            .navbox {
                display: none;
            }
            .mw-editsection {
                display: none;
            }
            .section {
                margin-bottom: 20px;
                padding: 10px;
                border-left: 3px solid #ddd;
            }
            .section h2 {
                margin-top: 0;
            }
            .reference-marker {
                font-size: 8pt;
                vertical-align: super;
                color: #0645ad;
            }
        </style>
        """

    def configure_pdfkit(self):
        """Configure pdfkit for the system"""
        for path in CONFIG['WKHTMLTOPDF_PATHS']:
            if os.path.exists(path):
                logging.info(f"Found wkhtmltopdf at: {path}")
                try:
                    import subprocess
                    result = subprocess.run([path, '--version'], capture_output=True, text=True)
                    if result.returncode == 0:
                        logging.info(f"wkhtmltopdf version: {result.stdout.strip()}")
                        return pdfkit.configuration(wkhtmltopdf=path)
                except Exception as e:
                    logging.warning(f"wkhtmltopdf test failed at {path}: {e}")
                    continue
        
        try:
            path = shutil.which('wkhtmltopdf')
            if path:
                logging.info(f"Found wkhtmltopdf in PATH: {path}")
                return pdfkit.configuration(wkhtmltopdf=path)
        except Exception as e:
            logging.warning(f"Error finding wkhtmltopdf in PATH: {e}")
        
        logging.warning("wkhtmltopdf not found. PDF generation will be disabled.")
        return None

    def clean_filename(self, text, max_length=150):
        """Sanitize filenames for Windows/Linux compatibility"""
        if not text:
            return "unknown"
        
        text = unquote(text)
        text = re.sub(r'[<>:"/\\|?*]', "_", text)
        text = re.sub(r'\s+', ' ', text).strip()
        if len(text) > max_length:
            text = text[:max_length].rsplit(' ', 1)[0]
        return text or "unknown"

    def get_domain_from_url(self, url):
        """Extract domain from URL for per-domain rate limiting"""
        try:
            parsed = urlparse(url)
            return parsed.netloc or 'default'
        except:
            return 'default'

    def check_global_rate_limit(self):
        """Check and enforce global rate limit (requests per minute)"""
        current_time = time.time()
        
        if current_time - self.minute_start >= 60:
            self.request_counter = 0
            self.minute_start = current_time
            return True
        
        if self.request_counter >= CONFIG['MAX_REQUESTS_PER_MINUTE']:
            sleep_time = 60 - (current_time - self.minute_start)
            if sleep_time > 0:
                logging.warning(f"Global rate limit reached. Sleeping for {sleep_time:.1f} seconds")
                time.sleep(sleep_time)
                self.request_counter = 0
                self.minute_start = time.time()
            return True
        
        return True

    def get_backoff_time(self, domain):
        """Calculate backoff time based on previous failures"""
        if domain in self.domain_failures:
            failures = self.domain_failures[domain]
            backoff = min(
                CONFIG['BACKOFF_MULTIPLIER'] ** failures * CONFIG['REQUEST_DELAY'],
                CONFIG['MAX_BACKOFF_SECONDS']
            )
            return backoff
        return 0

    def record_failure(self, domain):
        """Record a failure for a domain to trigger backoff"""
        self.domain_failures[domain] = self.domain_failures.get(domain, 0) + 1
        logging.warning(f"Recorded failure for {domain}. Total failures: {self.domain_failures[domain]}")

    def record_success(self, domain):
        """Reset failure count on successful request"""
        if domain in self.domain_failures:
            del self.domain_failures[domain]

    def polite_request(self, url, is_media=False):
        """Enhanced polite request with comprehensive rate limiting"""
        if self.shutdown_flag:
            raise Exception("Shutdown in progress")
        
        domain = self.get_domain_from_url(url)
        
        with self.request_semaphore:
            self.check_global_rate_limit()
            
            backoff_time = self.get_backoff_time(domain)
            if backoff_time > 0:
                logging.info(f"Backing off for {backoff_time:.1f}s due to previous failures on {domain}")
                time.sleep(backoff_time)
            
            if domain in self.domain_last_request:
                if 'upload.wikimedia.org' in domain:
                    domain_delay = CONFIG['REQUEST_DELAY'] * 3
                else:
                    domain_delay = CONFIG['REQUEST_DELAY']
                
                elapsed = time.time() - self.domain_last_request[domain]
                if elapsed < domain_delay:
                    time.sleep(domain_delay - elapsed)
            
            self.domain_last_request[domain] = time.time()
            self.request_counter += 1
            
            headers = {
                'User-Agent': self.user_agent,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Connection': 'keep-alive',
            }
            
            if is_media:
                headers['Accept'] = 'image/webp,image/apng,image/*,*/*;q=0.8'
                headers['Referer'] = 'https://en.wikipedia.org/'
            
            try:
                response = requests.get(url, headers=headers, timeout=CONFIG['TIMEOUT'], stream=is_media)
                response.raise_for_status()
                
                self.record_success(domain)
                
                return response
                
            except requests.exceptions.RequestException as e:
                if hasattr(e, 'response') and e.response:
                    if e.response.status_code == 429:
                        retry_after = int(e.response.headers.get('Retry-After', 60))
                        
                        cursor = self.db_conn.cursor()
                        cursor.execute('''
                        INSERT INTO rate_limit_log (domain, retry_after)
                        VALUES (?, ?)
                        ''', (domain, retry_after))
                        self.db_conn.commit()
                        
                        logging.warning(f"Rate limited on {domain}. Retry after: {retry_after}s")
                        
                        self.record_failure(domain)
                        
                        time.sleep(retry_after)
                        
                        return self.polite_request(url, is_media)
                    elif e.response.status_code in [500, 502, 503, 504]:
                        self.record_failure(domain)
                        time.sleep(CONFIG['REQUEST_DELAY'] * 2)
                        return self.polite_request(url, is_media)
                
                raise

    def get_proper_media_url(self, url, media_type):
        """Get the proper URL for downloading media, handling Wikimedia special cases"""
        if 'upload.wikimedia.org' not in url:
            return url
        
        # For Wikimedia Commons, we need to handle thumbnails differently
        if '/thumb/' in url:
            # Format: .../thumb/a/ab/Filename.jpg/800px-Filename.jpg
            parts = url.split('/thumb/')
            if len(parts) == 2:
                base_part = parts[0]
                path_part = parts[1]
                
                # The path part contains the original path and the thumbnail filename
                path_segments = path_part.split('/')
                if len(path_segments) >= 3:
                    # The original path is the first two segments (e.g., 'a/ab')
                    original_path = '/'.join(path_segments[:2])
                    # The filename is in the last segment after removing size prefix
                    filename_part = path_segments[-1]
                    # Remove size prefix (e.g., '800px-')
                    if '-' in filename_part:
                        filename = filename_part.split('-', 1)[1]
                    else:
                        filename = filename_part
                    
                    # Construct original file URL
                    return f"{base_part}/{original_path}/{filename}"
        
        # If it's not a thumbnail or we couldn't parse it, return the original
        return url

    def get_original_media_url(self, url):
        """Convert thumbnail URL to original image URL"""
        if not url:
            return url
            
        # Handle Wikimedia Commons thumbnails
        if 'commons.wikimedia.org' in url and '/thumb/' in url:
            # Pattern: https://upload.wikimedia.org/wikipedia/commons/thumb/a/ab/Filename.jpg/800px-Filename.jpg
            match = re.match(r'(https?://[^/]+/wikipedia/commons/)(?:thumb/)?(.*?)(?:/\d+px-.*)?$', url)
            if match:
                base = match.group(1)
                path = match.group(2)
                # Remove trailing thumbnail part if present
                path = re.sub(r'/\d+px-.*$', '', path)
                return f"{base}{path}"
        
        # Handle Wikipedia thumbnails
        if 'wikipedia.org' in url and '/thumb/' in url:
            # Pattern: /thumb/path/to/file/filename.ext/widthpx-filename.ext
            match = re.match(r'(.*)/thumb/(.*)/(\d+)px-(.*)', url)
            if match:
                base_path, file_path, width, filename = match.groups()
                return f"{base_path}/{file_path}"
        
        return url

    def get_thumbnail_url(self, url, width=None):
        """Convert to thumbnail URL for images"""
        if not url or not CONFIG['USE_THUMBNAILS']:
            return url
        
        width = width or CONFIG['THUMBNAIL_WIDTH']
        
        if 'upload.wikimedia.org' in url:
            # If it's already a thumbnail, return as is
            if '/thumb/' in url:
                return url
            
            # Parse the URL to create thumbnail URL
            parsed = urlparse(url)
            path_parts = parsed.path.split('/')
            
            # Wikimedia Commons structure: /wikipedia/commons/a/ab/Filename.jpg
            if len(path_parts) >= 5:
                # Find where the filename is
                filename = path_parts[-1]
                # The path before filename contains the directory structure
                dir_path = '/'.join(path_parts[:-1])
                # Create thumbnail path: /wikipedia/commons/thumb/a/ab/Filename.jpg/widthpx-Filename.jpg
                thumb_path = f"{dir_path}/thumb/{path_parts[-2]}/{path_parts[-1]}/{width}px-{filename}"
                return parsed._replace(path=thumb_path).geturl()
        
        return url

    def get_file_extension_from_url(self, url):
        """Extract file extension from URL"""
        parsed = urlparse(url)
        path = unquote(parsed.path)
        ext = os.path.splitext(path)[1].lower()
        if ext.startswith('.'):
            ext = ext[1:]
        return ext if ext else None

    def get_media_type_from_extension(self, extension):
        """Determine media type from file extension"""
        extension = extension.lower()
        if extension in CONFIG['MEDIA_TYPES']['images']:
            return 'image'
        elif extension in CONFIG['MEDIA_TYPES']['videos']:
            return 'video'
        elif extension in CONFIG['MEDIA_TYPES']['audio']:
            return 'audio'
        return None

    def extract_image_metadata(self, img_element, soup):
        """Extract metadata from image element"""
        metadata = {
            'title': None,
            'alt_text': None,
            'caption': None,
            'description': None,
            'license_info': None,
            'artist': None,
            'credit': None,
            'source': None,
            'width': None,
            'height': None
        }
        
        # Extract alt text
        metadata['alt_text'] = img_element.get('alt')
        
        # Extract title
        metadata['title'] = img_element.get('title')
        
        # Look for figure/figcaption
        parent = img_element.find_parent(['figure', 'div', 'span'])
        if parent:
            figcaption = parent.find('figcaption')
            if figcaption:
                metadata['caption'] = figcaption.get_text(strip=True)
            
            # Look for image metadata in parent
            metadata_elem = parent.find('div', class_=re.compile(r'(metadata|info|details)'))
            if metadata_elem:
                metadata['description'] = metadata_elem.get_text(strip=True)
        
        # Look for license information
        license_link = img_element.find_next('a', href=re.compile(r'(license|creativecommons|publicdomain)'))
        if license_link:
            metadata['license_info'] = license_link.get('href')
        
        # Get dimensions if available
        width = img_element.get('width')
        height = img_element.get('height')
        if width and width.isdigit():
            metadata['width'] = int(width)
        if height and height.isdigit():
            metadata['height'] = int(height)
        
        return metadata

    def download_resource(self, url, save_path, max_retries=3):
        """Download a resource with comprehensive retry logic"""
        if self.shutdown_flag:
            return False
        
        domain = self.get_domain_from_url(url)
        
        for attempt in range(max_retries):
            try:
                # Add headers to mimic a browser
                headers = {
                    'User-Agent': self.user_agent,
                    'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Referer': 'https://en.wikipedia.org/',
                }
                
                response = requests.get(url, headers=headers, timeout=CONFIG['TIMEOUT'], stream=True)
                response.raise_for_status()
                
                # Check content length before downloading
                content_length = response.headers.get('Content-Length')
                if content_length and int(content_length) > CONFIG['MAX_MEDIA_SIZE']:
                    logging.warning(f"File too large: {url} ({int(content_length) / (1024*1024):.1f}MB)")
                    return False
                
                # Check content type
                content_type = response.headers.get('Content-Type', '')
                if not any(media_type in content_type for media_type in ['image', 'video', 'audio', 'octet-stream', 'binary']):
                    logging.warning(f"Unexpected content type for {url}: {content_type}")
                
                total_size = 0
                with open(save_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if self.shutdown_flag:
                            return False
                        if chunk:
                            f.write(chunk)
                            total_size += len(chunk)
                            if total_size > CONFIG['MAX_MEDIA_SIZE']:
                                logging.warning(f"File exceeded size limit during download: {url}")
                                if os.path.exists(save_path):
                                    os.remove(save_path)
                                return False
                
                # Verify file was downloaded completely
                if os.path.exists(save_path) and os.path.getsize(save_path) > 0:
                    return True
                else:
                    logging.warning(f"Downloaded empty file: {url}")
                    return False
                    
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1}/{max_retries} failed for {url}: {str(e)}")
                
                if attempt < max_retries - 1:
                    sleep_time = (CONFIG['BACKOFF_MULTIPLIER'] ** attempt) * 5
                    sleep_time = min(sleep_time, CONFIG['MAX_BACKOFF_SECONDS'])
                    logging.info(f"Retrying in {sleep_time} seconds...")
                    time.sleep(sleep_time)
            
            except Exception as e:
                logging.error(f"Unexpected error downloading {url}: {str(e)}")
                if attempt < max_retries - 1:
                    sleep_time = (CONFIG['BACKOFF_MULTIPLIER'] ** attempt) * 5
                    sleep_time = min(sleep_time, CONFIG['MAX_BACKOFF_SECONDS'])
                    time.sleep(sleep_time)
        
        return False

    def generate_file_hash(self, file_path):
        """Generate SHA-256 hash of file contents"""
        if not os.path.exists(file_path):
            return None
            
        sha256 = hashlib.sha256()
        try:
            with open(file_path, 'rb') as f:
                while True:
                    if self.shutdown_flag:
                        return None
                    data = f.read(65536)
                    if not data:
                        break
                    sha256.update(data)
            return sha256.hexdigest()
        except Exception as e:
            logging.error(f"Error generating hash for {file_path}: {e}")
            return None

    def get_image_dimensions(self, file_path):
        """Get image dimensions using PIL"""
        try:
            with Image.open(file_path) as img:
                return img.size
        except Exception as e:
            logging.debug(f"Could not get image dimensions for {file_path}: {e}")
            return (None, None)

    def get_media_duration(self, file_path, media_type):
        """Get duration for audio/video files"""
        if media_type in ['audio', 'video']:
            try:
                audio = AudioSegment.from_file(file_path)
                return len(audio) / 1000.0  # Convert to seconds
            except Exception as e:
                logging.debug(f"Could not get duration for {file_path}: {e}")
        return None

    def download_media_element(self, element, article_dir, article_title, article_id=None):
        """Download a single media element with comprehensive metadata extraction"""
        if self.shutdown_flag or not CONFIG['DOWNLOAD_MEDIA']:
            return None
            
        try:
            # Determine media type and extract source URL
            if element.name == 'img':
                src = element.get('src') or element.get('data-src') or element.get('data-file-url')
                media_type = 'image'
            elif element.name == 'video':
                src = element.get('src') or element.get('data-src')
                if not src:
                    source = element.find('source')
                    if source:
                        src = source.get('src') or source.get('data-src')
                media_type = 'video'
            elif element.name == 'audio':
                src = element.get('src') or element.get('data-src')
                if not src:
                    source = element.find('source')
                    if source:
                        src = source.get('src') or source.get('data-src')
                media_type = 'audio'
            else:
                return None
            
            if not src:
                return None
            
            # Convert to absolute URL
            if src.startswith('//'):
                media_url = 'https:' + src
            elif src.startswith('/'):
                media_url = urljoin(CONFIG['BASE_URL'], src)
            else:
                media_url = src
            
            # Skip if media type is disabled
            media_type_plural = f'{media_type}s'
            if media_type_plural not in CONFIG['MEDIA_TYPES'] or not CONFIG['MEDIA_TYPES'][media_type_plural]:
                return None
            
            # Get original URL (for thumbnails)
            original_url = self.get_original_media_url(media_url)
            
            # Handle Wikimedia URLs properly
            download_url = self.get_proper_media_url(media_url, media_type)
            
            # Extract filename and extension
            parsed_url = urlparse(original_url)
            path = unquote(parsed_url.path)
            original_filename = os.path.basename(path).split('?')[0]
            
            # Try to determine file extension
            file_extension = self.get_file_extension_from_url(original_url)
            if not file_extension:
                file_extension = 'bin'
            
            # Extract metadata
            metadata = {}
            if media_type == 'image':
                metadata = self.extract_image_metadata(element, None)
            
            # Generate title from various attributes
            title = (
                element.get('alt') or 
                element.get('title') or 
                metadata.get('title') or
                element.get('aria-label') or
                (element.parent.get('title') if element.parent else None) or
                os.path.splitext(original_filename)[0].replace('_', ' ').title()
            )
            
            # Clean title for filename
            clean_title = self.clean_filename(title)
            if not clean_title or clean_title == 'unknown':
                clean_title = f"media_{int(time.time())}"
            
            # Create filename with proper extension
            final_filename = f"{clean_title}.{file_extension}"
            save_dir = os.path.join(article_dir, 'media', media_type_plural)
            os.makedirs(save_dir, exist_ok=True)
            save_path = os.path.join(save_dir, final_filename)
            
            # Handle filename conflicts
            counter = 1
            while os.path.exists(save_path):
                name_part = f"{clean_title}_{counter}"
                final_filename = f"{name_part}.{file_extension}"
                save_path = os.path.join(save_dir, final_filename)
                counter += 1
            
            cursor = self.db_conn.cursor()
            
            # Check if already downloaded
            cursor.execute(
                "SELECT id, local_path, file_hash FROM media_files WHERE url = ? OR original_url = ?",
                (media_url, original_url)
            )
            existing = cursor.fetchone()
            if existing:
                if os.path.exists(existing[1]):
                    logging.info(f"Media already exists: {title}")
                    return {
                        'id': existing[0],
                        'url': media_url,
                        'original_url': original_url,
                        'local_path': existing[1],
                        'media_type': media_type,
                        'title': title,
                        'filename': final_filename,
                        'file_extension': file_extension
                    }
                else:
                    cursor.execute('DELETE FROM media_files WHERE id = ?', (existing[0],))
                    self.db_conn.commit()
            
            logging.info(f"Downloading {media_type}: {title}")
            
            # Download the file
            if self.download_resource(download_url, save_path):
                file_size = os.path.getsize(save_path)
                file_hash = self.generate_file_hash(save_path)
                
                # Get additional metadata
                width, height = None, None
                duration = None
                
                if media_type == 'image':
                    width, height = self.get_image_dimensions(save_path)
                elif media_type in ['audio', 'video']:
                    duration = self.get_media_duration(save_path, media_type)
                
                # Insert into database
                cursor.execute('''
                INSERT INTO media_files 
                (article_id, media_type, url, original_url, thumbnail_url, local_path, filename, file_extension, 
                 file_size, file_hash, width, height, duration, title, alt_text, caption, description, 
                 license_info, artist, credit, source, metadata, download_status, download_attempts, download_timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'completed', 0, ?)
                ''', (
                    article_id,
                    media_type,
                    media_url,
                    original_url,
                    media_url if '/thumb/' in media_url else None,
                    save_path,
                    final_filename,
                    file_extension,
                    file_size,
                    file_hash,
                    width,
                    height,
                    duration,
                    title,
                    metadata.get('alt_text'),
                    metadata.get('caption'),
                    metadata.get('description'),
                    metadata.get('license_info'),
                    metadata.get('artist'),
                    metadata.get('credit'),
                    metadata.get('source'),
                    json.dumps(metadata),
                    datetime.datetime.now()
                ))
                
                media_id = cursor.lastrowid
                self.db_conn.commit()
                
                logging.info(f"Downloaded {media_type}: {title} ({file_size / 1024:.1f}KB)")
                
                return {
                    'id': media_id,
                    'url': media_url,
                    'original_url': original_url,
                    'local_path': save_path,
                    'media_type': media_type,
                    'title': title,
                    'filename': final_filename,
                    'file_extension': file_extension,
                    'file_size': file_size,
                    'width': width,
                    'height': height,
                    'duration': duration
                }
            else:
                # Record failure
                cursor.execute('''
                INSERT INTO media_files 
                (article_id, media_type, url, original_url, title, download_status, download_attempts, last_attempt, error_message)
                VALUES (?, ?, ?, ?, ?, 'failed', 1, ?, 'Download failed after retries')
                ''', (article_id, media_type, media_url, original_url, title, datetime.datetime.now()))
                
                self.db_conn.commit()
                
                if os.path.exists(save_path):
                    os.remove(save_path)
                return None
                
        except Exception as e:
            logging.error(f"Error downloading media from {element}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def process_article_media(self, soup, article_dir, article_title, article_id=None):
        """Process all media in an article with limits and categorization"""
        if self.shutdown_flag or not CONFIG['DOWNLOAD_MEDIA']:
            return {'images': [], 'videos': [], 'audio': []}
        
        # Find all media elements
        media_elements = []
        media_elements.extend(soup.find_all('img'))
        media_elements.extend(soup.find_all('video'))
        media_elements.extend(soup.find_all('audio'))
        
        # Filter out very small icons and unwanted elements
        filtered_elements = []
        for element in media_elements:
            if element.name == 'img':
                # Skip very small images (likely icons)
                width = element.get('width')
                height = element.get('height')
                if width and width.isdigit() and int(width) < 50:
                    continue
                if height and height.isdigit() and int(height) < 50:
                    continue
                
                # Skip data: URLs
                src = element.get('src', '')
                if src.startswith('data:'):
                    continue
            
            filtered_elements.append(element)
        
        # Apply media limit
        if len(filtered_elements) > CONFIG['MAX_MEDIA_PER_ARTICLE']:
            logging.info(f"Limiting media for {article_title} from {len(filtered_elements)} to {CONFIG['MAX_MEDIA_PER_ARTICLE']}")
            filtered_elements = filtered_elements[:CONFIG['MAX_MEDIA_PER_ARTICLE']]
        
        logging.info(f"Processing {len(filtered_elements)} media elements in {article_title}")
        
        results = {
            'images': [],
            'videos': [],
            'audio': []
        }
        
        successful = 0
        failed = 0
        
        for element in filtered_elements:
            if self.shutdown_flag:
                break
            
            try:
                result = self.download_media_element(element, article_dir, article_title, article_id)
                if result:
                    media_type = result['media_type']
                    if media_type == 'image':
                        results['images'].append(result)
                    elif media_type == 'video':
                        results['videos'].append(result)
                    elif media_type == 'audio':
                        results['audio'].append(result)
                    successful += 1
                else:
                    failed += 1
                
                # Dynamic delay based on success/failure rate
                if failed > successful and failed > 3:
                    time.sleep(CONFIG['REQUEST_DELAY'] * 2)
                else:
                    time.sleep(CONFIG['REQUEST_DELAY'])
                    
            except Exception as e:
                failed += 1
                logging.error(f"Media processing error: {str(e)}")
        
        total_media = successful
        total_size = sum(m.get('file_size', 0) for m in results['images'] + results['videos'] + results['audio'])
        
        logging.info(f"Media processing complete for {article_title}: {successful} successful, {failed} failed")
        logging.info(f"Downloaded: {len(results['images'])} images, {len(results['videos'])} videos, {len(results['audio'])} audio files")
        if total_size > 0:
            logging.info(f"Total media size: {total_size / (1024*1024):.2f}MB")
        
        return results

    def extract_references(self, soup, article_id):
        """Extract references/citations from the article"""
        if not CONFIG['EXTRACT_REFERENCES']:
            return []
        
        references = []
        cursor = self.db_conn.cursor()
        
        try:
            ref_section = None
            for heading in soup.find_all(['h2', 'h3']):
                if 'reference' in heading.get_text(strip=True).lower() or 'citation' in heading.get_text(strip=True).lower():
                    ref_section = heading.find_next('div', class_='reflist') or heading.find_next('ol', class_='references')
                    if ref_section:
                        break
            
            if not ref_section:
                ref_section = soup.find('div', class_='reflist') or soup.find('ol', class_='references')
            
            if ref_section:
                ref_items = ref_section.find_all('li')
                
                for i, ref_item in enumerate(ref_items, 1):
                    ref_text = ref_item.get_text(strip=True)
                    
                    ref_link = ref_item.find('a', class_='external text')
                    ref_url = ref_link.get('href') if ref_link else None
                    
                    cite_template = ref_item.find('span', class_='citation')
                    if cite_template:
                        cite_text = cite_template.get_text()
                        
                        author_match = re.search(r'([A-Za-z\s,]+?)\s*\(', cite_text)
                        title_match = re.search(r'"([^"]+)"', cite_text)
                        pub_match = re.search(r'([A-Za-z\s]+?)(?:\s*\d{4})', cite_text)
                        date_match = re.search(r'(\d{4})', cite_text)
                        isbn_match = re.search(r'ISBN\s+([\d-]+)', cite_text)
                        doi_match = re.search(r'doi:\s*([^\s,]+)', cite_text, re.IGNORECASE)
                        
                        ref_author = author_match.group(1) if author_match else None
                        ref_title = title_match.group(1) if title_match else None
                        ref_publication = pub_match.group(1) if pub_match else None
                        ref_date = date_match.group(1) if date_match else None
                        isbn = isbn_match.group(1) if isbn_match else None
                        doi = doi_match.group(1) if doi_match else None
                    else:
                        ref_author = None
                        ref_title = None
                        ref_publication = None
                        ref_date = None
                        isbn = None
                        doi = None
                    
                    cursor.execute('''
                    INSERT INTO article_references 
                    (article_id, ref_number, ref_text, ref_url, ref_title, ref_author, ref_publication, ref_date, isbn, doi, raw_citation)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        article_id, i, ref_text, ref_url, ref_title, ref_author, 
                        ref_publication, ref_date, isbn, doi, cite_text if cite_template else None
                    ))
                    
                    ref_id = cursor.lastrowid
                    references.append({
                        'id': ref_id,
                        'number': i,
                        'text': ref_text,
                        'url': ref_url,
                        'title': ref_title,
                        'author': ref_author,
                        'publication': ref_publication,
                        'date': ref_date,
                        'isbn': isbn,
                        'doi': doi
                    })
            
            if not references:
                ref_links = soup.find_all('a', href=re.compile(r'#cite_note'))
                ref_ids = set()
                for link in ref_links:
                    href = link.get('href', '')
                    match = re.search(r'#cite_note-([^-]+)', href)
                    if match:
                        ref_ids.add(match.group(1))
                
                if ref_ids:
                    ref_section = soup.find('div', class_='reflist') or soup.find('ol', class_='references')
                    if ref_section:
                        for ref_key in ref_ids:
                            ref_item = ref_section.find('li', id=re.compile(f'cite_note-{ref_key}'))
                            if ref_item:
                                ref_text = ref_item.get_text(strip=True)
                                cursor.execute('''
                                INSERT INTO article_references (article_id, ref_number, ref_text, raw_citation)
                                VALUES (?, ?, ?, ?)
                                ''', (article_id, len(references) + 1, ref_text, ref_text))
                                ref_id = cursor.lastrowid
                                references.append({
                                    'id': ref_id,
                                    'number': len(references) + 1,
                                    'text': ref_text
                                })
            
            self.db_conn.commit()
            logging.info(f"Extracted {len(references)} references from article")
            
            return references
            
        except Exception as e:
            logging.error(f"Error extracting references: {str(e)}")
            return []

    def extract_paragraphs_with_references_and_media(self, soup, article_id, section_id, section_element):
        """Extract paragraphs from a section and link them to references and media"""
        if self.shutdown_flag:
            return []
        
        paragraphs = []
        cursor = self.db_conn.cursor()
        
        try:
            para_elements = section_element.find_all(['p'], recursive=False)
            
            for p_num, para in enumerate(para_elements, 1):
                para_text = para.get_text(strip=True)
                
                if not para_text or len(para_text) < 20:
                    continue
                
                clean_para = re.sub(r'\[\d+\]', '', para_text)
                clean_para = re.sub(r'\[\w+\]', '', clean_para)
                clean_para = re.sub(r'\s+', ' ', clean_para).strip()
                
                ref_markers = []
                ref_ids = []
                
                ref_links = para.find_all('a', href=re.compile(r'#cite_note'))
                for link in ref_links:
                    href = link.get('href', '')
                    marker = link.get_text(strip=True)
                    
                    match = re.search(r'#cite_note-([^-]+)', href)
                    if match:
                        ref_key = match.group(1)
                        
                        cursor.execute('''
                        SELECT id FROM article_references 
                        WHERE article_id = ? AND (raw_citation LIKE ? OR ref_text LIKE ?)
                        ''', (article_id, f'%{ref_key}%', f'%{ref_key}%'))
                        
                        ref_result = cursor.fetchone()
                        if ref_result:
                            ref_ids.append(ref_result[0])
                            ref_markers.append(marker)
                
                cite_templates = para.find_all('span', class_='citation')
                for cite in cite_templates:
                    cite_text = cite.get_text()
                    
                    cursor.execute('''
                    SELECT id FROM article_references 
                    WHERE article_id = ? AND raw_citation = ?
                    ''', (article_id, cite_text))
                    
                    ref_result = cursor.fetchone()
                    if ref_result and ref_result[0] not in ref_ids:
                        ref_ids.append(ref_result[0])
                        ref_markers.append('[citation]')
                
                media_ids = []
                media_elements = para.find_all(['img', 'video', 'audio'])
                
                for media_elem in media_elements:
                    src = media_elem.get('src') or media_elem.get('data-src')
                    if src:
                        if src.startswith('//'):
                            media_url = 'https:' + src
                        elif src.startswith('/'):
                            media_url = urljoin(CONFIG['BASE_URL'], src)
                        else:
                            media_url = src
                        
                        cursor.execute('''
                        SELECT id FROM media_files WHERE url = ? OR original_url = ?
                        ''', (media_url, media_url))
                        
                        media_result = cursor.fetchone()
                        if media_result:
                            media_ids.append(media_result[0])
                
                word_count = len(clean_para.split())
                char_count = len(clean_para)
                
                cursor.execute('''
                INSERT INTO article_paragraphs 
                (article_id, section_id, paragraph_number, paragraph_text, word_count, char_count, reference_ids, media_ids)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (article_id, section_id, p_num, clean_para, word_count, char_count, 
                      json.dumps(ref_ids) if ref_ids else None,
                      json.dumps(media_ids) if media_ids else None))
                
                para_id = cursor.lastrowid
                
                for ref_id in ref_ids:
                    cursor.execute('''
                    INSERT INTO paragraph_references (paragraph_id, reference_id, ref_marker)
                    VALUES (?, ?, ?)
                    ''', (para_id, ref_id, marker if 'marker' in locals() else None))
                
                for position, media_id in enumerate(media_ids):
                    cursor.execute('''
                    INSERT INTO paragraph_media (paragraph_id, media_id, position)
                    VALUES (?, ?, ?)
                    ''', (para_id, media_id, position))
                
                paragraphs.append({
                    'id': para_id,
                    'number': p_num,
                    'text': clean_para,
                    'word_count': word_count,
                    'char_count': char_count,
                    'reference_ids': ref_ids,
                    'reference_markers': ref_markers,
                    'media_ids': media_ids
                })
            
            return paragraphs
            
        except Exception as e:
            logging.error(f"Error extracting paragraphs: {str(e)}")
            return []

    def extract_article_sections(self, soup, article_id):
        """Extract article content by sections with proper hierarchy and references"""
        content = soup.find('div', {'id': 'mw-content-text'}) or soup.find('body') or soup
        
        unwanted_selectors = [
            'script', 'style', 'noscript', '.navbox', '.mw-editsection', 
            '.metadata', '.ambox', '.sistersitebox', '.vertical-navbox', 
            '.navbox-styles', '[role="navigation"]', '.hatnote', '.external', 
            '.mw-empty-elt', '.toc', '#toc', '.thumb', '.thumbinner'
        ]
        
        for selector in unwanted_selectors:
            for element in soup.select(selector):
                element.decompose()
        
        sections = []
        current_section = {
            'title': 'Introduction',
            'level': 1,
            'number': 0,
            'element': None,
            'paragraphs': [],
            'content': [],
            'formatted_content': []
        }
        
        elements = content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'p', 'ul', 'ol', 'table'])
        
        for element in elements:
            if element.name.startswith('h'):
                if current_section['content'] or current_section.get('element'):
                    sections.append(current_section)
                
                level = int(element.name[1])
                title = element.get_text(strip=True)
                
                if title and not any(x in title.lower() for x in ['references', 'notes', 'citations', 'external links', 'see also']):
                    current_section = {
                        'title': title,
                        'level': level,
                        'number': len(sections) + 1,
                        'element': element,
                        'paragraphs': [],
                        'content': [],
                        'formatted_content': []
                    }
            
            elif element.name in ['p', 'ul', 'ol'] and current_section:
                text = element.get_text(strip=True)
                if text and len(text) > 20:
                    clean_text = re.sub(r'\[\d+\]', '', text)
                    clean_text = re.sub(r'\[\w+\]', '', clean_text)
                    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
                    
                    if clean_text:
                        current_section['content'].append(clean_text)
                        
                        if element.name == 'p':
                            current_section['formatted_content'].append(clean_text)
                        elif element.name in ['ul', 'ol']:
                            bullet = "-" if element.name == 'ul' else "•"
                            for li in element.find_all('li', recursive=False):
                                li_text = li.get_text(strip=True)
                                if li_text:
                                    li_text = re.sub(r'\[\d+\]', '', li_text)
                                    li_text = re.sub(r'\[\w+\]', '', li_text)
                                    li_text = re.sub(r'\s+', ' ', li_text).strip()
                                    current_section['formatted_content'].append(f"  {bullet} {li_text}")
            
            elif element.name == 'table' and current_section:
                caption = element.find('caption')
                if caption:
                    current_section['formatted_content'].append(f"\n[Table: {caption.get_text(strip=True)}]")
        
        if current_section['content'] or current_section.get('element'):
            sections.append(current_section)
        
        cursor = self.db_conn.cursor()
        
        for section in sections:
            if section.get('element'):
                paragraphs = self.extract_paragraphs_with_references_and_media(
                    soup, article_id, None, section['element'].parent
                )
                section['paragraphs'] = paragraphs
        
        logging.info(f"Extracted {len(sections)} sections with paragraphs")
        return sections

    def save_article_sections_with_paragraphs(self, sections, article_dir, title, article_id, references):
        """Save article sections and paragraphs as text files"""
        if not CONFIG['SAVE_TEXT_FILE']:
            return None
        
        try:
            sections_dir = os.path.join(article_dir, 'sections')
            paragraphs_dir = os.path.join(article_dir, 'paragraphs')
            references_dir = os.path.join(article_dir, 'references')
            
            os.makedirs(sections_dir, exist_ok=True)
            os.makedirs(paragraphs_dir, exist_ok=True)
            os.makedirs(references_dir, exist_ok=True)
            
            cursor = self.db_conn.cursor()
            base_filename = self.clean_filename(title)
            
            sections_text_path = os.path.join(sections_dir, f"{base_filename}_all_sections.txt")
            all_sections_content = []
            total_paragraphs = 0
            
            for section in sections:
                section_num = section['number']
                section_title = section['title']
                section_level = section['level']
                
                safe_section_title = self.clean_filename(section_title)
                section_filename = f"{base_filename}_section_{section_num:02d}_{safe_section_title}.txt"
                section_path = os.path.join(sections_dir, section_filename)
                
                section_content = [
                    f"{'#' * section_level} {section_title}",
                    "=" * 50,
                    "",
                ]
                section_content.extend(section['content'])
                
                with open(section_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(section_content))
                
                full_text = '\n'.join(section['content'])
                word_count = len(full_text.split())
                char_count = len(full_text)
                
                cursor.execute('''
                INSERT INTO article_sections 
                (article_id, section_number, section_title, section_level, text_content, formatted_content, text_file_path, word_count, char_count)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    article_id, 
                    section_num, 
                    section_title, 
                    section_level, 
                    full_text,
                    '\n'.join(section['formatted_content']),
                    section_path,
                    word_count,
                    char_count
                ))
                
                section_db_id = cursor.lastrowid
                
                section_paragraphs = []
                for para in section.get('paragraphs', []):
                    para_num = para['number']
                    para_text = para['text']
                    
                    para_filename = f"{base_filename}_section_{section_num:02d}_para_{para_num:03d}.txt"
                    para_path = os.path.join(paragraphs_dir, para_filename)
                    
                    with open(para_path, 'w', encoding='utf-8') as f:
                        f.write(f"Section {section_num}: {section_title}\n")
                        f.write("=" * 50 + "\n\n")
                        f.write(para_text)
                        
                        if para.get('reference_ids'):
                            f.write("\n\nReferences in this paragraph:\n")
                            f.write("-" * 30 + "\n")
                            for ref_id in para['reference_ids']:
                                ref = next((r for r in references if r['id'] == ref_id), None)
                                if ref:
                                    f.write(f"[{ref['number']}] {ref['text'][:200]}...\n")
                    
                    cursor.execute('''
                    UPDATE article_paragraphs 
                    SET section_id = ? WHERE id = ?
                    ''', (section_db_id, para['id']))
                    
                    section_paragraphs.append(f"Paragraph {para_num}:\n{para_text}")
                    total_paragraphs += 1
                
                all_sections_content.extend([
                    f"\n{'#' * section_level} Section {section_num}: {section_title}",
                    "-" * 60,
                    full_text,
                    f"\n[This section has {len(section.get('paragraphs', []))} paragraphs]",
                    "\n" + "=" * 60 + "\n"
                ])
                
                logging.info(f"Saved section {section_num}: {section_title} ({word_count} words, {len(section.get('paragraphs', []))} paragraphs)")
            
            with open(sections_text_path, 'w', encoding='utf-8') as f:
                f.write(f"Wikipedia Article: {title}\n")
                f.write("=" * 60 + "\n\n")
                f.write('\n'.join(all_sections_content))
                f.write(f"\n\nTotal Sections: {len(sections)}")
                f.write(f"\nTotal Paragraphs: {total_paragraphs}")
                f.write(f"\nTotal References: {len(references)}")
                f.write(f"\nArchived: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            
            if references:
                refs_path = os.path.join(references_dir, f"{base_filename}_references.txt")
                with open(refs_path, 'w', encoding='utf-8') as f:
                    f.write(f"References for: {title}\n")
                    f.write("=" * 60 + "\n\n")
                    
                    for ref in references:
                        f.write(f"[{ref['number']}] ")
                        if ref.get('author'):
                            f.write(f"{ref['author']}. ")
                        if ref.get('title'):
                            f.write(f"\"{ref['title']}\". ")
                        if ref.get('publication'):
                            f.write(f"{ref['publication']}. ")
                        if ref.get('date'):
                            f.write(f"({ref['date']}). ")
                        if ref.get('url'):
                            f.write(f"\n    URL: {ref['url']}")
                        if ref.get('isbn'):
                            f.write(f"\n    ISBN: {ref['isbn']}")
                        if ref.get('doi'):
                            f.write(f"\n    DOI: {ref['doi']}")
                        f.write(f"\n\n    {ref['text']}\n")
                        f.write("-" * 40 + "\n\n")
                
                cursor.execute('''
                UPDATE articles SET references_text_path = ? WHERE id = ?
                ''', (refs_path, article_id))
            
            cursor.execute('''
            UPDATE articles SET sections_text_path = ?, total_sections = ?, total_paragraphs = ?, total_references = ? WHERE id = ?
            ''', (sections_text_path, len(sections), total_paragraphs, len(references), article_id))
            
            self.db_conn.commit()
            
            logging.info(f"Saved {len(sections)} sections, {total_paragraphs} paragraphs, and {len(references)} references for {title}")
            return sections_text_path
            
        except Exception as e:
            logging.error(f"Error saving sections for {title}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def extract_clean_text(self, soup):
        """Extract clean, readable text from BeautifulSoup object"""
        unwanted_selectors = [
            'script', 'style', 'noscript', '.navbox', '.mw-editsection', 
            '.reference', '.references', '.metadata',
            '.ambox', '.sistersitebox', '.vertical-navbox', '.navbox-styles',
            '[role="navigation"]', '.hatnote', '.external', '.mw-empty-elt'
        ]
        
        for selector in unwanted_selectors:
            for element in soup.select(selector):
                element.decompose()
        
        content = soup.find('div', {'id': 'mw-content-text'}) or soup.find('body') or soup
        
        text_parts = []
        
        for heading in content.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6']):
            level = int(heading.name[1]) if heading.name.startswith('h') else 1
            text = heading.get_text(strip=True)
            if text:
                text_parts.append(f"\n{'#' * level} {text}\n")
        
        for element in content.find_all(['p', 'li', 'td']):
            text = element.get_text(strip=True)
            if text and len(text) > 20:
                text = re.sub(r'\[\d+\]', '', text)
                text = re.sub(r'\[\w+\]', '', text)
                text = re.sub(r'\s+', ' ', text).strip()
                if text:
                    text_parts.append(text)
        
        full_text = '\n'.join(text_parts)
        
        if len(full_text) > CONFIG['TTS_MAX_LENGTH']:
            full_text = full_text[:CONFIG['TTS_MAX_LENGTH']] + "... [text truncated]"
        
        return full_text

    def extract_formatted_text(self, soup):
        """Extract formatted text with structure preserved"""
        unwanted_selectors = [
            'script', 'style', 'noscript', '.navbox', '.mw-editsection', 
            '.reference', '.references', '.metadata', '.ambox', 
            '.sistersitebox', '.vertical-navbox', '.navbox-styles',
            '[role="navigation"]', '.hatnote', '.external', '.mw-empty-elt'
        ]
        
        for selector in unwanted_selectors:
            for element in soup.select(selector):
                element.decompose()
        
        content = soup.find('div', {'id': 'mw-content-text'}) or soup.find('body') or soup
        
        formatted_parts = []
        
        title = soup.find('h1', {'id': 'firstHeading'})
        if title:
            formatted_parts.append(f"TITLE: {title.get_text(strip=True)}")
            formatted_parts.append("=" * 60)
        
        for element in content.find_all(recursive=True):
            if element.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                level = int(element.name[1])
                text = element.get_text(strip=True)
                if text:
                    formatted_parts.append(f"\n{'#' * level} {text}\n")
            
            elif element.name == 'p':
                text = element.get_text(strip=True)
                if text and len(text) > 10:
                    text = re.sub(r'\[\d+\]', '', text)
                    text = re.sub(r'\[\w+\]', '', text)
                    text = re.sub(r'\s+', ' ', text).strip()
                    formatted_parts.append(text)
            
            elif element.name in ['ul', 'ol']:
                for li in element.find_all('li', recursive=False):
                    text = li.get_text(strip=True)
                    if text:
                        bullet = "-" if element.name == 'ul' else "•"
                        formatted_parts.append(f"  {bullet} {text}")
                formatted_parts.append("")
        
        return '\n'.join(formatted_parts)

    def save_article_text(self, soup, article_dir, title, article_id):
        """Save article content as text files"""
        if not CONFIG['SAVE_TEXT_FILE']:
            return None, None
        
        try:
            text_dir = os.path.join(article_dir, 'text')
            os.makedirs(text_dir, exist_ok=True)
            
            base_filename = self.clean_filename(title)
            text_path = None
            formatted_text_path = None
            
            if CONFIG['TEXT_FORMAT'] in ['plain', 'both']:
                plain_text = self.extract_clean_text(soup)
                text_path = os.path.join(text_dir, f"{base_filename}.txt")
                
                with open(text_path, 'w', encoding='utf-8') as f:
                    f.write(f"Wikipedia Article: {title}\n")
                    f.write("=" * 50 + "\n\n")
                    f.write(plain_text)
                    f.write(f"\n\nSource: {CONFIG['BASE_URL']}/wiki/{title.replace(' ', '_')}")
                    f.write(f"\nArchived: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
                logging.info(f"Saved plain text: {text_path}")
            
            if CONFIG['TEXT_FORMAT'] in ['formatted', 'both']:
                formatted_text = self.extract_formatted_text(soup)
                formatted_text_path = os.path.join(text_dir, f"{base_filename}_formatted.txt")
                
                with open(formatted_text_path, 'w', encoding='utf-8') as f:
                    f.write(formatted_text)
                    f.write(f"\n\nSource URL: {CONFIG['BASE_URL']}/wiki/{title.replace(' ', '_')}")
                    f.write(f"\nArchive Date: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    f.write(f"\nText Version: Formatted")
                
                logging.info(f"Saved formatted text: {formatted_text_path}")
            
            cursor = self.db_conn.cursor()
            cursor.execute('''
            UPDATE articles SET text_path = ?, formatted_text_path = ? WHERE id = ?
            ''', (text_path, formatted_text_path, article_id))
            self.db_conn.commit()
            
            return text_path, formatted_text_path
            
        except Exception as e:
            logging.error(f"Error saving text files for {title}: {str(e)}")
            return None, None

    def split_text_into_chunks(self, text, chunk_size=4000):
        """Split text into chunks for TTS processing"""
        sentences = re.split(r'(?<=[.!?])\s+', text)
        chunks = []
        current_chunk = ""
        
        for sentence in sentences:
            if len(current_chunk) + len(sentence) <= chunk_size:
                current_chunk += sentence + " "
            else:
                if current_chunk:
                    chunks.append(current_chunk.strip())
                current_chunk = sentence + " "
        
        if current_chunk:
            chunks.append(current_chunk.strip())
        
        return chunks

    def generate_tts_audio(self, text, output_path, language='en', slow=False):
        """Generate TTS audio using gTTS"""
        try:
            with self.tts_lock:
                tts = gTTS(text=text, lang=language, slow=slow)
                tts.save(output_path)
            
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return True
            else:
                logging.error(f"TTS generation failed: {output_path} is empty or missing")
                return False
                
        except Exception as e:
            logging.error(f"TTS generation error: {e}")
            return False

    def add_silence_padding(self, audio_path, padding_ms=500):
        """Add silence padding to the beginning and end of audio"""
        try:
            audio = AudioSegment.from_file(audio_path)
            silence = AudioSegment.silent(duration=padding_ms)
            padded_audio = silence + audio + silence
            padded_audio.export(audio_path, format=CONFIG['TTS_AUDIO_FORMAT'])
            return True
        except Exception as e:
            logging.warning(f"Could not add silence padding to {audio_path}: {e}")
            return False

    def generate_section_tts(self, article_id):
        """Generate TTS audio for each section of an article and save in tts folder"""
        if self.shutdown_flag or not CONFIG['TTS_ENABLED']:
            return None

        cursor = self.db_conn.cursor()
        
        try:
            cursor.execute('''
            SELECT title, article_dir FROM articles WHERE id = ?
            ''', (article_id,))
            article = cursor.fetchone()
            
            if not article:
                return None
                
            title, article_dir = article
            
            cursor.execute('''
            SELECT id, section_number, section_title, text_content 
            FROM article_sections 
            WHERE article_id = ? 
            ORDER BY section_number
            ''', (article_id,))
            
            sections = cursor.fetchall()
            
            if not sections:
                logging.warning(f"No sections found for TTS in article: {title}")
                return None
            
            tts_dir = os.path.join(article_dir, 'tts')
            sections_tts_dir = os.path.join(tts_dir, 'sections')
            os.makedirs(sections_tts_dir, exist_ok=True)
            
            logging.info(f"Generating section TTS for: {title} ({len(sections)} sections)")
            
            section_audio_paths = []
            
            for section in sections:
                if self.shutdown_flag:
                    break
                    
                section_id, section_num, section_title, text_content = section
                
                if not text_content or len(text_content.strip()) < 50:
                    logging.warning(f"Section {section_num} has insufficient text, skipping TTS")
                    continue
                
                safe_section_title = self.clean_filename(section_title)
                section_filename = f"section_{section_num:02d}_{safe_section_title}.{CONFIG['TTS_AUDIO_FORMAT']}"
                section_audio_path = os.path.join(sections_tts_dir, section_filename)
                
                logging.info(f"Generating TTS for section {section_num}: {section_title}")
                
                chunks = self.split_text_into_chunks(text_content, CONFIG['TTS_CHUNK_SIZE'])
                
                if len(chunks) == 1:
                    if self.generate_tts_audio(
                        text_content,
                        section_audio_path,
                        language=CONFIG['TTS_LANGUAGE'],
                        slow=CONFIG['TTS_SPEED'] < 1.0
                    ):
                        self.add_silence_padding(section_audio_path)
                        file_size = os.path.getsize(section_audio_path)
                        
                        cursor.execute('''
                        INSERT INTO tts_segments (article_id, section_id, segment_number, text_content, audio_path, file_size)
                        VALUES (?, ?, 1, ?, ?, ?)
                        ''', (article_id, section_id, text_content[:500] + "..." if len(text_content) > 500 else text_content, section_audio_path, file_size))
                        
                        section_audio_paths.append(section_audio_path)
                        
                        cursor.execute('''
                        UPDATE article_sections SET tts_file_path = ? WHERE id = ?
                        ''', (section_audio_path, section_id))
                        
                        logging.info(f"Generated TTS for section {section_num}")
                        
                else:
                    chunk_audio_paths = []
                    
                    for i, chunk in enumerate(chunks, 1):
                        chunk_filename = f"section_{section_num:02d}_{safe_section_title}_part_{i:02d}.{CONFIG['TTS_AUDIO_FORMAT']}"
                        chunk_path = os.path.join(sections_tts_dir, chunk_filename)
                        
                        if self.generate_tts_audio(
                            chunk,
                            chunk_path,
                            language=CONFIG['TTS_LANGUAGE'],
                            slow=CONFIG['TTS_SPEED'] < 1.0
                        ):
                            self.add_silence_padding(chunk_path)
                            file_size = os.path.getsize(chunk_path)
                            
                            cursor.execute('''
                            INSERT INTO tts_segments (article_id, section_id, segment_number, text_content, audio_path, file_size)
                            VALUES (?, ?, ?, ?, ?, ?)
                            ''', (article_id, section_id, i, chunk[:500] + "..." if len(chunk) > 500 else chunk, chunk_path, file_size))
                            
                            chunk_audio_paths.append(chunk_path)
                            
                            time.sleep(1)
                    
                    if chunk_audio_paths:
                        try:
                            combined_audio = AudioSegment.empty()
                            for chunk_path in chunk_audio_paths:
                                combined_audio += AudioSegment.from_file(chunk_path)
                            
                            combined_audio.export(
                                section_audio_path,
                                format=CONFIG['TTS_AUDIO_FORMAT'],
                                bitrate=CONFIG['TTS_BITRATE']
                            )
                            
                            file_size = os.path.getsize(section_audio_path)
                            
                            cursor.execute('''
                            UPDATE article_sections SET tts_file_path = ? WHERE id = ?
                            ''', (section_audio_path, section_id))
                            
                            section_audio_paths.append(section_audio_path)
                            logging.info(f"Generated combined TTS for section {section_num}")
                            
                        except Exception as e:
                            logging.error(f"Error combining chunks for section {section_num}: {e}")
                            if chunk_audio_paths:
                                cursor.execute('''
                                UPDATE article_sections SET tts_file_path = ? WHERE id = ?
                                ''', (chunk_audio_paths[0], section_id))
                                section_audio_paths.append(chunk_audio_paths[0])
                
                time.sleep(2)
            
            if len(section_audio_paths) > 1 and not self.shutdown_flag:
                try:
                    full_audio_path = os.path.join(tts_dir, f"{self.clean_filename(title)}_full.{CONFIG['TTS_AUDIO_FORMAT']}")
                    
                    intro_text = f"Wikipedia article: {title}. This article has {len(sections)} sections."
                    intro_path = os.path.join(tts_dir, "intro.mp3")
                    
                    if self.generate_tts_audio(intro_text, intro_path, language=CONFIG['TTS_LANGUAGE']):
                        combined_audio = AudioSegment.from_file(intro_path)
                        
                        pause = AudioSegment.silent(duration=1000)
                        
                        for i, section_path in enumerate(section_audio_paths):
                            section_audio = AudioSegment.from_file(section_path)
                            if i > 0:
                                combined_audio += pause
                            combined_audio += section_audio
                        
                        combined_audio.export(
                            full_audio_path,
                            format=CONFIG['TTS_AUDIO_FORMAT'],
                            bitrate=CONFIG['TTS_BITRATE']
                        )
                        
                        cursor.execute('''
                        UPDATE articles SET tts_path = ? WHERE id = ?
                        ''', (full_audio_path, article_id))
                        
                        logging.info(f"Generated full article TTS: {full_audio_path}")
                        
                except Exception as e:
                    logging.error(f"Error creating full article TTS: {e}")
            
            self.db_conn.commit()
            
            cursor.execute('''
            UPDATE archive_stats SET tts_files_generated = tts_files_generated + ? WHERE date = ?
            ''', (len(section_audio_paths), datetime.datetime.now().date()))
            self.db_conn.commit()
            
            logging.info(f"Completed section TTS generation for {title}: {len(section_audio_paths)} sections processed")
            
            return section_audio_paths
            
        except Exception as e:
            logging.error(f"Section TTS generation failed for article {article_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def save_table(self, table, table_num, article_dir, article_title):
        """Save table as HTML and CSV"""
        if self.shutdown_flag:
            return None
            
        try:
            tables_dir = os.path.join(article_dir, 'tables')
            os.makedirs(tables_dir, exist_ok=True)
            
            base_filename = f"{self.clean_filename(article_title)}_table_{table_num}"
            
            html_path = os.path.join(tables_dir, f"{base_filename}.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(f"<h2>{article_title} - Table {table_num}</h2>")
                f.write(str(table))
            
            csv_path = os.path.join(tables_dir, f"{base_filename}.csv")
            rows = []
            
            for tr in table.find_all('tr'):
                cells = []
                for th in tr.find_all('th'):
                    cells.append(th.get_text(strip=True))
                for td in tr.find_all('td'):
                    cells.append(td.get_text(strip=True))
                if cells:
                    rows.append(cells)
            
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerows(rows)
            
            logging.info(f"Saved table {table_num} for {article_title}")
            return {
                'html_path': html_path, 
                'csv_path': csv_path, 
                'table_number': table_num,
                'row_count': len(rows)
            }
            
        except Exception as e:
            logging.error(f"Error saving table {table_num}: {str(e)}")
            return None

    def process_article_tables(self, content, article_dir, article_title):
        """Process all tables in an article"""
        if self.shutdown_flag:
            return []
        
        tables = content.find_all('table', class_=lambda x: x not in ['navbox', 'metadata', 'ambox'])
        
        if len(tables) > 10:
            logging.info(f"Limiting tables for {article_title} from {len(tables)} to 10")
            tables = tables[:10]
        
        logging.info(f"Found {len(tables)} tables in {article_title}")
        results = []
        
        for i, table in enumerate(tables, 1):
            if self.shutdown_flag:
                break
            try:
                result = self.save_table(table, i, article_dir, article_title)
                if result:
                    results.append(result)
                time.sleep(0.5)
            except Exception as e:
                logging.error(f"Table processing error for table {i}: {str(e)}")
        
        return results

    def generate_pdf(self, article_id):
        """Generate a properly formatted PDF version of the article with fixed SVG handling"""
        if self.shutdown_flag or not self.pdfkit_config:
            return None

        cursor = self.db_conn.cursor()
        
        try:
            cursor.execute('''
            SELECT title, url, html_path, article_dir FROM articles WHERE id = ?
            ''', (article_id,))
            article = cursor.fetchone()
            
            if not article:
                return None
                
            title, url, html_path, article_dir = article
            
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            
            unwanted_selectors = ['script', 'style', 'noscript', '.navbox', '.mw-editsection']
            for selector in unwanted_selectors:
                for element in soup.select(selector):
                    element.decompose()
            
            for img in soup.find_all('img'):
                src = img.get('src') or ''
                if not src:
                    img.decompose()
                    continue
                
                # Skip if src is None or empty
                if not src:
                    img.decompose()
                    continue
                    
                cursor.execute('''
                SELECT local_path FROM media_files WHERE url LIKE ? OR url LIKE ? LIMIT 1
                ''', (f'%{src}%', f'%{os.path.basename(src)}%'))
                media_file = cursor.fetchone()
                
                if media_file and media_file[0] and os.path.exists(media_file[0]):
                    try:
                        file_path = media_file[0]
                        
                        # Check if file exists and is readable
                        if not os.path.exists(file_path):
                            img.decompose()
                            continue
                        
                        if file_path.lower().endswith('.svg'):
                            # For SVGs, just link to the file instead of converting
                            img['src'] = f"file://{file_path}"
                            img['style'] = 'max-width: 100%; height: auto;'
                        else:
                            with open(file_path, 'rb') as img_file:
                                img_data = img_file.read()
                            img_type = mimetypes.guess_type(file_path)[0] or 'image/jpeg'
                            base64_data = base64.b64encode(img_data).decode('utf-8')
                            img['src'] = f"data:{img_type};base64,{base64_data}"
                            img['style'] = 'max-width: 100%; height: auto;'
                            
                    except Exception as e:
                        logging.warning(f"Couldn't process image {file_path}: {str(e)}")
                        img.decompose()
                else:
                    img.decompose()
            
            header_html = f"""
            <div style="font-size: 8pt; text-align: left; width: 100%; border-bottom: 1px solid #ccc; padding-bottom: 5px;">
                <span>{title}</span>
                <span style="float: right;">Page <span class="pageNumber"></span> of <span class="totalPages"></span></span>
            </div>
            """
            
            footer_html = f"""
            <div style="font-size: 7pt; text-align: center; width: 100%; border-top: 1px solid #ccc; padding-top: 5px;">
                Source: <a href="{url}">{url}</a> | Generated on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
            </div>
            """
            
            content_div = soup.find('div', {'id': 'mw-content-text'}) or soup.find('body') or soup
            if not content_div:
                content_div = soup
            
            pdf_html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <meta charset="UTF-8">
                <title>{title}</title>
                {self.pdf_styles}
            </head>
            <body>
                {header_html}
                <div style="padding: 15px;">
                    <h1>{title}</h1>
                    {str(content_div)}
                </div>
                {footer_html}
            </body>
            </html>
            """
            
            pdf_dir = os.path.join(article_dir, 'pdf')
            os.makedirs(pdf_dir, exist_ok=True)
            pdf_path = os.path.join(pdf_dir, f"{self.clean_filename(title)}.pdf")
            
            pdfkit.from_string(
                pdf_html,
                pdf_path,
                options=CONFIG['PDF_OPTIONS'],
                configuration=self.pdfkit_config
            )
            
            if os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 1000:
                cursor.execute('''
                UPDATE articles SET pdf_path = ? WHERE id = ?
                ''', (pdf_path, article_id))
                self.db_conn.commit()
                file_size = os.path.getsize(pdf_path)
                logging.info(f"Successfully generated PDF: {pdf_path} ({file_size / 1024:.1f}KB)")
                return pdf_path
            else:
                logging.error("Failed to generate PDF - file is empty or too small")
                if os.path.exists(pdf_path):
                    os.remove(pdf_path)
                return None
                
        except Exception as e:
            logging.error(f"PDF generation failed for article {article_id}: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    def scrape_article(self, url):
        """Scrape and save a Wikipedia article with all content including media"""
        if self.shutdown_flag:
            return None
            
        cursor = self.db_conn.cursor()
        article_id = None
        
        try:
            cursor.execute('SELECT id, status FROM articles WHERE url = ?', (url,))
            existing = cursor.fetchone()
            if existing:
                if existing[1] == 'completed':
                    logging.info(f"Article already exists: {url}")
                    return existing[0]
                else:
                    cursor.execute('DELETE FROM articles WHERE id = ?', (existing[0],))
                    self.db_conn.commit()
            
            logging.info(f"Starting to scrape: {url}")
            
            response = self.polite_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            title_element = soup.find('h1', {'id': 'firstHeading'})
            if not title_element:
                logging.error(f"Could not find title for: {url}")
                return None
                
            title = title_element.text.strip()
            logging.info(f"Scraping article: {title}")
            
            article_dir = os.path.join(CONFIG['ROOT_DIR'], self.clean_filename(title))
            os.makedirs(article_dir, exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'media', 'images'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'media', 'videos'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'media', 'audio'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'tables'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'pdf'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'tts'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'text'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'sections'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'paragraphs'), exist_ok=True)
            os.makedirs(os.path.join(article_dir, 'references'), exist_ok=True)
            
            content = soup.find('div', {'id': 'mw-content-text'})
            if not content:
                logging.error(f"Could not find content for: {title}")
                return None
            
            last_updated_elem = soup.find('li', {'id': 'footer-info-lastmod'})
            last_updated = last_updated_elem.text.replace('This page was last edited on ', '').strip() if last_updated_elem else "Unknown"
            
            summary = ''
            if content:
                first_para = content.find('p')
                if first_para:
                    summary = first_para.get_text(strip=True)[:500] + '...' if len(first_para.get_text(strip=True)) > 500 else first_para.get_text(strip=True)
            
            cursor.execute('''
            INSERT INTO articles (title, url, summary, last_updated, article_dir, html_path, status)
            VALUES (?, ?, ?, ?, ?, ?, 'processing')
            ''', (title, url, summary, last_updated, article_dir, None))
            article_id = cursor.lastrowid
            
            html_path = os.path.join(article_dir, f"{self.clean_filename(title)}.html")
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(response.text)
            
            cursor.execute('''
            UPDATE articles SET html_path = ? WHERE id = ?
            ''', (html_path, article_id))
            
            media_results = self.process_article_media(soup, article_dir, title, article_id)
            
            references = self.extract_references(soup, article_id)
            
            sections = self.extract_article_sections(soup, article_id)
            
            tables = self.process_article_tables(content, article_dir, title)
            
            self.save_article_sections_with_paragraphs(sections, article_dir, title, article_id, references)
            
            text_path, formatted_text_path = self.save_article_text(soup, article_dir, title, article_id)
            
            for table in tables:
                cursor.execute('''
                INSERT INTO article_tables (article_id, table_number, html_path, csv_path)
                VALUES (?, ?, ?, ?)
                ''', (article_id, table['table_number'], table['html_path'], table['csv_path']))
            
            total_images = len(media_results.get('images', []))
            total_videos = len(media_results.get('videos', []))
            total_audio = len(media_results.get('audio', []))
            total_media_size = sum(
                m.get('file_size', 0) for m in 
                media_results.get('images', []) + 
                media_results.get('videos', []) + 
                media_results.get('audio', [])
            )
            
            cursor.execute('''
            UPDATE articles SET 
                status = 'completed',
                total_images = ?,
                total_videos = ?,
                total_audio = ?,
                total_media_size = ?
            WHERE id = ?
            ''', (total_images, total_videos, total_audio, total_media_size, article_id))
            
            self.db_conn.commit()
            
            if not self.shutdown_flag:
                self.generate_pdf(article_id)
            
            if not self.shutdown_flag and CONFIG['TTS_ENABLED']:
                self.generate_section_tts(article_id)
            
            logging.info(f"Successfully processed: {title} (ID: {article_id})")
            logging.info(f"  - {len(sections)} sections")
            logging.info(f"  - {len(references)} references")
            logging.info(f"  - {total_images} images ({sum(m.get('file_size', 0) for m in media_results.get('images', [])) / 1024:.1f}KB)")
            logging.info(f"  - {total_videos} videos")
            logging.info(f"  - {total_audio} audio files")
            logging.info(f"  - {len(tables)} tables")
            
            return article_id
            
        except Exception as e:
            self.db_conn.rollback()
            logging.error(f"Error scraping {url}: {str(e)}")
            import traceback
            traceback.print_exc()
            
            if article_id:
                try:
                    cursor.execute('DELETE FROM articles WHERE id = ?', (article_id,))
                    self.db_conn.commit()
                except:
                    pass
            return None

    def get_current_events_articles(self):
        """Get articles linked from current events page"""
        if self.shutdown_flag:
            return []
            
        try:
            url = "https://en.wikipedia.org/wiki/Portal:Current_events" 
            logging.info(f"Fetching current events from: {url}")
            
            response = self.polite_request(url)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            articles = set()
            content = soup.find('div', {'id': 'mw-content-text'})
            
            if content:
                current_events_links = content.select('.description a, .vevent a, .summary a')
                
                for link in current_events_links:
                    href = link.get('href', '')
                    if (href.startswith('/wiki/') and 
                        ':' not in href and 
                        '#' not in href and
                        not href.startswith('/wiki/File:') and
                        not href.startswith('/wiki/Special:') and
                        not href.startswith('/wiki/Template:')):
                        
                        article_url = urljoin(CONFIG['BASE_URL'], href)
                        articles.add(article_url)
                        
                        if len(articles) >= CONFIG['MAX_ARTICLES']:
                            break
            
            article_list = list(articles)[:CONFIG['MAX_ARTICLES']]
            logging.info(f"Found {len(article_list)} unique articles from current events")
            return article_list
            
        except Exception as e:
            logging.error(f"Error fetching current events: {str(e)}")
            return []

    def update_stats(self):
        """Update archive statistics"""
        try:
            cursor = self.db_conn.cursor()
            today = datetime.datetime.now().date()
            
            cursor.execute('''
            SELECT 
                COUNT(*) as article_count,
                (SELECT COUNT(*) FROM media_files WHERE DATE(download_timestamp) = ? AND media_type = 'image' AND download_status = 'completed') as images_count,
                (SELECT COUNT(*) FROM media_files WHERE DATE(download_timestamp) = ? AND media_type = 'video' AND download_status = 'completed') as videos_count,
                (SELECT COUNT(*) FROM media_files WHERE DATE(download_timestamp) = ? AND media_type = 'audio' AND download_status = 'completed') as audio_count,
                (SELECT COUNT(*) FROM article_tables) as table_count,
                (SELECT COALESCE(SUM(file_size), 0) FROM media_files WHERE DATE(download_timestamp) = ? AND download_status = 'completed') as total_size,
                (SELECT COALESCE(SUM(total_sections), 0) FROM articles WHERE DATE(scrape_timestamp) = ?) as total_sections,
                (SELECT COALESCE(SUM(total_paragraphs), 0) FROM articles WHERE DATE(scrape_timestamp) = ?) as total_paragraphs,
                (SELECT COALESCE(SUM(total_references), 0) FROM articles WHERE DATE(scrape_timestamp) = ?) as total_references,
                (SELECT COUNT(*) FROM tts_segments WHERE DATE(created_at) = ?) as tts_count
            FROM articles WHERE DATE(scrape_timestamp) = ?
            ''', (today, today, today, today, today, today, today, today, today))
            
            stats = cursor.fetchone()
            
            cursor.execute('''
            INSERT OR REPLACE INTO archive_stats 
            (date, articles_archived, images_downloaded, videos_downloaded, audio_downloaded, tables_saved, total_size_bytes, total_sections, total_paragraphs, total_references, tts_files_generated)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (today, stats[0], stats[1], stats[2], stats[3], stats[4], stats[5], stats[6], stats[7], stats[8], stats[9]))
            
            self.db_conn.commit()
            logging.info(f"Stats updated: {stats[0]} articles, {stats[1]} images, {stats[2]} videos, {stats[3]} audio, "
                        f"{stats[4]} tables, {stats[5] / (1024*1024):.1f}MB, {stats[6]} sections, {stats[7]} paragraphs, {stats[8]} references, {stats[9]} TTS files")
            
        except Exception as e:
            logging.error(f"Error updating stats: {e}")

    def run(self):
        """Main execution method"""
        if self.shutdown_flag:
            return
            
        logging.info("Starting Wikipedia Archiver")
        start_time = time.time()
        
        try:
            articles = self.get_current_events_articles()
            
            if not articles:
                logging.warning("No articles found in current events")
                return
            
            total_articles = len(articles)
            processed = 0
            failed = 0
            
            logging.info(f"Starting to process {total_articles} articles with {CONFIG['MAX_WORKERS']} workers")
            
            with ThreadPoolExecutor(
                max_workers=CONFIG['MAX_WORKERS'],
                thread_name_prefix='Archiver'
            ) as executor:
                future_to_url = {
                    executor.submit(self.scrape_article, url): url 
                    for url in articles
                }
                
                for future in as_completed(future_to_url):
                    if self.shutdown_flag:
                        logging.info("Shutdown detected, stopping...")
                        executor.shutdown(wait=False)
                        break
                        
                    url = future_to_url[future]
                    try:
                        article_id = future.result(timeout=300)
                        if article_id:
                            processed += 1
                        else:
                            failed += 1
                        
                        progress = (processed + failed) / total_articles * 100
                        logging.info(f"Progress: {progress:.1f}% - {processed}/{total_articles} succeeded, {failed} failed")
                        
                    except Exception as e:
                        failed += 1
                        logging.error(f"Unexpected error processing {url}: {e}")
                        import traceback
                        traceback.print_exc()
            
            if not self.shutdown_flag:
                self.update_stats()
            
            elapsed_time = time.time() - start_time
            logging.info(f"Archive completed! {processed}/{total_articles} articles processed successfully in {elapsed_time:.2f} seconds")
            
        except Exception as e:
            logging.error(f"Unexpected error in main execution: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.close_db_connections()

def main():
    """Main entry point with exception handling"""
    try:
        archiver = WikipediaArchiver()
        archiver.run()
    except KeyboardInterrupt:
        logging.info("Archiver stopped by user")
    except Exception as e:
        logging.error(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()