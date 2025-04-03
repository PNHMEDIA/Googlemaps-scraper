#!/usr/bin/env python3

from selenium import webdriver
# from selenium.webdriver.chrome.service import Service # Service is often optional now
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException
import time
import os
import csv
import json
import logging
import re
import math
import concurrent.futures
from datetime import datetime
import threading
import random
from urllib.parse import quote, urlparse, parse_qs
import sys
import traceback
import argparse
from pathlib import Path
import shutil
# import socket # Not used in the final version
import hashlib
import statistics
from collections import Counter, defaultdict

# --- Optional Dependency Imports ---
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Pandas not available. Excel export disabled.")

try:
    import matplotlib.pyplot as plt
    import numpy as np
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False
    print("Matplotlib not available. Visualization features disabled.")

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("tqdm not available. Progress bars disabled.")
    # Define a dummy tqdm if not available
    class MockTqdm:
        """A dummy tqdm class for when the real tqdm is not installed."""
        def __init__(self, iterable=None, total=None, desc=None, unit=None, **kwargs):
            self.iterable = iterable
            self.total = total
            self.n = 0
            if desc:
                print(desc) # Print description once

        def update(self, n=1):
            self.n += n
            # Simple text progress update
            if self.total:
                print(f"... processed {self.n}/{self.total}", end='\r')

        def close(self):
            print() # Newline after finishing

        def __iter__(self):
            return iter(self.iterable)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            self.close()

    # Assign the dummy class to tqdm
    tqdm = MockTqdm


try:
    import colorama
    colorama.init()
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    print("Colorama not available. Colored output disabled.")

# --- Global Constants ---
VERSION = "3.2.0" # Updated version for parallel processing
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:122.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1"
]

# --- Utility Functions ---
def ensure_directories_exist():
    """Ensure all required directories exist, creating them if necessary."""
    required_dirs = ["logs", "debug", "results", "temp", "grid_data", "reports", "cache", "configs"]
    print("--- Ensuring Directories ---")
    for directory in required_dirs:
        try:
            dir_path = Path(directory)
            dir_path.mkdir(parents=True, exist_ok=True)
            print(f"✓ Directory '{directory}' is ready")
        except Exception as e:
            print(f"⚠️ Warning: Could not create directory '{directory}': {e}")
            fallback_dir = Path(f"_{directory}")
            try:
                fallback_dir.mkdir(parents=True, exist_ok=True)
                print(f"  -> Using fallback directory: '{fallback_dir}'")
            except Exception as e2:
                print(f"  -> ❌ Could not create fallback directory: {e2}")
                print("  -> Will use current directory for outputs if needed.")
    print("----------------------------\n")


def hash_string(text):
    """Create a hash of a string for caching purposes"""
    return hashlib.md5(text.encode()).hexdigest()

# --- Logging Setup ---
class ColorFormatter(logging.Formatter):
    """Custom formatter with colors for different log levels"""
    def __init__(self, use_color=True):
        super().__init__(fmt="%(asctime)s - %(levelname)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        self.use_color = use_color and COLORAMA_AVAILABLE
        if self.use_color:
            from colorama import Fore, Style
            self.colors = {
                'DEBUG': Fore.CYAN,
                'INFO': Fore.GREEN,
                'WARNING': Fore.YELLOW,
                'ERROR': Fore.RED,
                'CRITICAL': Fore.RED + Style.BRIGHT
            }
            self.reset = Style.RESET_ALL
        else:
            self.colors = {level: "" for level in ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']}
            self.reset = ""

    def format(self, record):
        levelname = record.levelname
        color = self.colors.get(levelname, "")
        # Ensure message is a string
        if not isinstance(record.msg, str):
             record.msg = str(record.msg)
        # Format the message using the base class
        formatted_message = super().format(record)
        return f"{color}{formatted_message}{self.reset}"


def setup_logging(session_id):
    """Set up advanced logging with multiple handlers and colored output"""
    log_dir = Path("logs")
    try:
        log_dir.mkdir(exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not create logs directory: {e}. Using fallback '_logs'.")
        log_dir = Path("_logs")
        try:
            log_dir.mkdir(exist_ok=True)
        except Exception:
            print("Warning: Could not create fallback logs directory. Using current directory.")
            log_dir = Path(".")

    # Configure main logger
    logger = logging.getLogger("GoogleMapsScraper")
    logger.setLevel(logging.DEBUG)
    if logger.hasHandlers(): logger.handlers.clear()

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColorFormatter())

    # Main file handler
    main_log_file = log_dir / f"gmaps_scraper_{session_id}.log"
    main_file_handler = logging.FileHandler(main_log_file, encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)
    main_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')) # Added threadName

    # Error file handler
    error_log_file = log_dir / f"gmaps_errors_{session_id}.log"
    error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_file_handler.setLevel(logging.WARNING)
    error_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s\n%(pathname)s:%(lineno)d\n')) # Added threadName

    logger.addHandler(console_handler)
    logger.addHandler(main_file_handler)
    logger.addHandler(error_file_handler)

    # Grid debug logger
    grid_logger = logging.getLogger("GridDebug")
    grid_logger.setLevel(logging.DEBUG)
    if grid_logger.hasHandlers(): grid_logger.handlers.clear()
    grid_log_file = log_dir / f"grid_debug_{session_id}.log"
    grid_handler = logging.FileHandler(grid_log_file, encoding='utf-8')
    grid_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    grid_logger.addHandler(grid_handler)

    # Business data logger
    business_logger = logging.getLogger("BusinessData")
    business_logger.setLevel(logging.INFO)
    if business_logger.hasHandlers(): business_logger.handlers.clear()
    business_log_file = log_dir / f"business_data_{session_id}.log"
    business_handler = logging.FileHandler(business_log_file, encoding='utf-8')
    business_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s')) # Keep simple for data logging
    business_logger.addHandler(business_handler)

    return logger, grid_logger, business_logger

# --- Core Classes ---
class BrowserPool:
    """Manages a pool of browser instances for parallel processing"""
    def __init__(self, max_browsers=5, headless=True, proxy_list=None, debug=False):
        self.max_browsers = max_browsers
        self.headless = headless
        self.proxy_list = proxy_list or []
        self.debug = debug
        self.lock = threading.Lock()
        self.browsers = {} # Use dict: id -> browser instance
        self.browser_in_use = {} # id -> bool
        self.browser_health = {} # id -> dict
        self.next_browser_id = 0
        self.logger = logging.getLogger("GoogleMapsScraper")

    def get_browser(self, timeout=60): # Increased timeout
        """Get an available browser from the pool, creating one if needed"""
        start_time = time.time()
        thread_id = threading.get_ident()
        self.logger.debug(f"Thread {thread_id} requesting browser...")

        while time.time() - start_time < timeout:
            with self.lock:
                # Check for an available existing browser
                for browser_id, in_use in self.browser_in_use.items():
                    if not in_use:
                        self.browser_in_use[browser_id] = True
                        self.logger.debug(f"Thread {thread_id} acquired existing browser #{browser_id}")
                        return browser_id

                # If no available browser, try to create a new one if pool not full
                if len(self.browsers) < self.max_browsers:
                    try:
                        new_id = self.next_browser_id
                        browser = self._create_browser()
                        self.browsers[new_id] = browser
                        self.browser_in_use[new_id] = True
                        self.browser_health[new_id] = {"errors": 0, "pages_loaded": 0}
                        self.next_browser_id += 1
                        self.logger.info(f"Thread {thread_id} created and acquired new browser #{new_id} (Pool size: {len(self.browsers)}/{self.max_browsers})")
                        return new_id
                    except Exception as e:
                        self.logger.error(f"Thread {thread_id} failed to create browser: {e}", exc_info=True)
                        # Don't immediately retry creation in case of systemic issue
                        time.sleep(2) # Wait before next attempt cycle

            # If no browser acquired or created, wait before checking again
            self.logger.debug(f"Thread {thread_id} waiting for browser...")
            time.sleep(random.uniform(0.5, 1.5)) # Random sleep to avoid thundering herd

        self.logger.error(f"Thread {thread_id} timed out waiting for browser after {timeout}s")
        raise TimeoutError(f"No browser available in the pool within {timeout} seconds")

    def _create_browser(self):
        """Create a new browser instance"""
        options = Options()
        if self.headless:
            options.add_argument("--headless=new")
        options.add_argument("--window-size=1920,1080")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        options.add_argument("--log-level=3") # Suppress console logs from Chrome/Driver
        options.add_experimental_option('excludeSwitches', ['enable-logging']) # Further suppress logs

        # Random user agent
        options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")

        # Add proxy if available
        if self.proxy_list:
            proxy = random.choice(self.proxy_list)
            options.add_argument(f'--proxy-server={proxy}')
            self.logger.debug(f"Using proxy: {proxy}")

        # Create the browser
        # Consider adding Service object if chromedriver is not in PATH
        # service = Service('/path/to/chromedriver')
        # browser = webdriver.Chrome(service=service, options=options)
        browser = webdriver.Chrome(options=options)
        browser.set_page_load_timeout(45) # Increased page load timeout
        browser.set_script_timeout(45) # Increased script timeout
        return browser

    def release_browser(self, browser_id):
        """Mark a browser as available"""
        thread_id = threading.get_ident()
        with self.lock:
            if browser_id in self.browser_in_use:
                self.browser_in_use[browser_id] = False
                if browser_id in self.browser_health: # Check if health entry exists
                     self.browser_health[browser_id]["pages_loaded"] += 1
                self.logger.debug(f"Thread {thread_id} released browser #{browser_id}")
            else:
                 self.logger.warning(f"Thread {thread_id} tried to release non-existent/already released browser #{browser_id}")


    def report_error(self, browser_id):
        """Report an error with a browser, potentially recreating it"""
        thread_id = threading.get_ident()
        with self.lock:
            if browser_id not in self.browser_health:
                 self.logger.warning(f"Thread {thread_id} reported error for non-existent browser #{browser_id}")
                 return # Cannot report error for a browser that doesn't exist in the pool

            self.browser_health[browser_id]["errors"] += 1
            error_count = self.browser_health[browser_id]["errors"]
            self.logger.warning(f"Thread {thread_id} reported error for browser #{browser_id} (Error count: {error_count})")

            # If too many errors, recreate the browser
            if error_count >= 3:
                self.logger.warning(f"Browser #{browser_id} has {error_count} errors, recreating...")
                try:
                    old_browser = self.browsers.get(browser_id)
                    if old_browser:
                        try:
                            old_browser.quit()
                        except Exception as quit_err:
                             self.logger.warning(f"Error quitting old browser #{browser_id}: {quit_err}")
                        del self.browsers[browser_id] # Remove from dict

                    # Create replacement
                    new_browser = self._create_browser()
                    self.browsers[browser_id] = new_browser # Replace in dict with same ID
                    self.browser_health[browser_id] = {"errors": 0, "pages_loaded": 0} # Reset health
                    # Keep browser marked as in_use as the calling thread still holds it
                    self.logger.info(f"Thread {thread_id} successfully recreated browser #{browser_id}")

                except Exception as e:
                    self.logger.error(f"Thread {thread_id} failed during recreation of browser #{browser_id}: {e}", exc_info=True)
                    # If recreation fails, remove the problematic browser ID entirely
                    if browser_id in self.browsers: del self.browsers[browser_id]
                    if browser_id in self.browser_in_use: del self.browser_in_use[browser_id]
                    if browser_id in self.browser_health: del self.browser_health[browser_id]
                    self.logger.error(f"Removed problematic browser ID {browser_id} from pool after recreation failure.")


    def get_driver(self, browser_id):
        """Get the actual driver instance for a browser_id"""
        # No lock needed for read if assignment is atomic, but safer with lock
        with self.lock:
             return self.browsers.get(browser_id) # Use .get for safety


    def close_all(self):
        """Close all browsers in the pool"""
        self.logger.info(f"Closing all {len(self.browsers)} browsers in the pool...")
        with self.lock:
            for browser_id, browser in list(self.browsers.items()): # Iterate over a copy
                try:
                    browser.quit()
                    self.logger.debug(f"Closed browser #{browser_id}")
                except Exception as e:
                    self.logger.warning(f"Error closing browser #{browser_id}: {e}")
                # Clean up entries even if quit fails
                if browser_id in self.browser_in_use: del self.browser_in_use[browser_id]
                if browser_id in self.browser_health: del self.browser_health[browser_id]
                # Don't delete from self.browsers while iterating its copy, clear at end

            self.browsers.clear()
            self.browser_in_use.clear()
            self.browser_health.clear()
            self.logger.info("Browser pool closed and cleared.")


class DataCache:
    """Cache manager for storing and retrieving data to reduce network and processing load"""
    def __init__(self, enabled=True, max_age_hours=24):
        self.enabled = enabled
        self.max_age_seconds = max_age_hours * 3600
        self.cache_dir = Path("cache")
        try:
             self.cache_dir.mkdir(exist_ok=True)
        except Exception:
             self.cache_dir = Path("_cache") # Fallback
             try:
                 self.cache_dir.mkdir(exist_ok=True)
             except Exception:
                 print("Warning: Could not create cache directory. Cache disabled.")
                 self.enabled = False

        self.logger = logging.getLogger("GoogleMapsScraper")
        self.lock = threading.Lock() # Lock for file access
        if self.enabled:
             self._clear_old_cache()

    def _get_cache_path(self, cache_key):
        """Get the filesystem path for a cache key"""
        hashed_key = hash_string(cache_key)
        return self.cache_dir / f"{hashed_key}.json"

    def _clear_old_cache(self):
        """Remove cache entries older than max_age"""
        if not self.enabled: return
        now = time.time()
        count = 0
        try:
            for cache_file in self.cache_dir.glob("*.json"):
                try:
                    if now - cache_file.stat().st_mtime > self.max_age_seconds:
                        cache_file.unlink()
                        count += 1
                except Exception as e:
                     self.logger.warning(f"Error processing cache file {cache_file}: {e}")
            if count > 0:
                self.logger.info(f"Cleared {count} old cache entries")
        except Exception as e:
             self.logger.error(f"Error during cache cleanup: {e}")


    def get(self, cache_key):
        """Get a value from cache if it exists and is not expired"""
        if not self.enabled: return None
        cache_path = self._get_cache_path(cache_key)
        try:
            with self.lock:
                if cache_path.exists():
                    if time.time() - cache_path.stat().st_mtime > self.max_age_seconds:
                        cache_path.unlink()
                        self.logger.debug(f"Cache expired for {cache_key[:30]}...")
                        return None
                    with open(cache_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.logger.debug(f"Cache hit for {cache_key[:30]}...")
                    return data
        except Exception as e:
            self.logger.warning(f"Error reading from cache ({cache_path}): {e}")
        return None

    def set(self, cache_key, value):
        """Store a value in the cache"""
        if not self.enabled: return
        cache_path = self._get_cache_path(cache_key)
        try:
            with self.lock:
                # Write to a temporary file first
                temp_path = cache_path.with_suffix(".tmp")
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(value, f, ensure_ascii=False)
                # Atomically replace the old file
                temp_path.replace(cache_path)
            self.logger.debug(f"Cached data for {cache_key[:30]}...")
        except Exception as e:
            self.logger.warning(f"Error writing to cache ({cache_path}): {e}")
            # Clean up temp file if it exists
            if temp_path.exists():
                 try: temp_path.unlink()
                 except: pass


    def invalidate(self, cache_key):
        """Remove a specific entry from the cache"""
        if not self.enabled: return
        cache_path = self._get_cache_path(cache_key)
        try:
            with self.lock:
                if cache_path.exists():
                    cache_path.unlink()
                    self.logger.debug(f"Invalidated cache for {cache_key[:30]}...")
        except Exception as e:
            self.logger.warning(f"Error invalidating cache ({cache_path}): {e}")


class ConsentHandler:
    """Advanced handler for various Google consent pages and popups"""
    def __init__(self, logger):
        self.logger = logger
        # More specific patterns first
        self.consent_patterns = [
            {"url_pattern": "consent.google.com", "severity": "high"},
            {"url_pattern": "accounts.google.com/signin/v2/identifier", "severity": "high"}, # Login page
            {"url_pattern": "accounts.google.com", "severity": "medium"}, # Other account pages
            {"url_pattern": "/maps/preview/consent", "severity": "medium"},
            {"url_pattern": "_/consentview", "severity": "medium"},
            {"url_pattern": "consent_flow", "severity": "medium"}
        ]
        # Common button texts (add more as needed)
        self.accept_texts = [
            "Accept all", "Accetta tutto", "Tout accepter", "Alle akzeptieren",
            "Aceptar todo", "Aceitar tudo", "Alles accepteren", "Acceptér alle",
            "I agree", "Sono d'accordo", "J'accepte", "Ich stimme zu",
            "Estoy de acuerdo", "Concordo", "Ik ga akkoord"
        ]

    def handle_consent(self, driver, take_screenshot=False, debug_dir=None):
        """Handle various Google consent pages and popups"""
        try:
            current_url = driver.current_url
            consent_detected = any(p["url_pattern"] in current_url for p in self.consent_patterns)

            if consent_detected:
                severity = next((p["severity"] for p in self.consent_patterns if p["url_pattern"] in current_url), "low")
                self.logger.info(f"⚠️ Detected consent/login page ({severity}): {current_url}")

                if take_screenshot and debug_dir:
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
                    screenshot_path = Path(debug_dir) / f"consent_{timestamp}.png"
                    try:
                        driver.save_screenshot(str(screenshot_path))
                        self.logger.info(f"Saved consent page screenshot to {screenshot_path}")
                    except Exception as e:
                        self.logger.warning(f"Error saving consent screenshot: {e}")

                # Try clicking common accept buttons
                if self._try_click_buttons(driver, self.accept_texts):
                    self.logger.info("Consent handled by clicking common accept button.")
                    time.sleep(random.uniform(1.5, 2.5)) # Wait for page redirect/update
                    return True

                # Add more specific handlers if needed (e.g., for forms)
                # if self._try_form_buttons(driver): return True

                self.logger.warning("Could not automatically handle consent/login page.")
                return False # Indicate consent page was detected but not handled

            # Check for cookie banners even if not on a full consent page
            if self._try_cookie_banners(driver):
                 self.logger.info("Handled a cookie banner.")
                 time.sleep(random.uniform(0.5, 1.0))
                 return True # Indicate a banner was handled (might not be full consent)

            return False # No consent page or banner detected/handled

        except Exception as e:
            self.logger.error(f"Error in consent handling: {e}", exc_info=True)
            return False

    def _try_click_buttons(self, driver, button_texts):
        """Try clicking buttons containing specific texts."""
        for text in button_texts:
            selectors = [
                f"//button[normalize-space()='{text}']",
                f"//button[contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text.lower()}')]",
                f"//div[@role='button' and normalize-space()='{text}']",
                f"//div[@role='button' and contains(translate(normalize-space(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), '{text.lower()}')]",
                f"//span[normalize-space()='{text}']//ancestor::button", # Text within a span inside a button
            ]
            for selector in selectors:
                try:
                    buttons = driver.find_elements(By.XPATH, selector)
                    for button in buttons:
                         # Check if button is visible and clickable
                         if button.is_displayed() and button.is_enabled():
                             try:
                                 button.click()
                                 self.logger.info(f"Clicked button with text '{text}' using selector: {selector}")
                                 return True
                             except Exception as click_err:
                                 self.logger.debug(f"Could not click button '{text}' found by {selector}: {click_err}")
                                 # Try JavaScript click as fallback
                                 try:
                                     driver.execute_script("arguments[0].click();", button)
                                     self.logger.info(f"Clicked button '{text}' using JavaScript fallback.")
                                     return True
                                 except Exception as js_click_err:
                                      self.logger.debug(f"JS click also failed for button '{text}': {js_click_err}")
                except Exception as find_err:
                    self.logger.debug(f"Error finding button with selector {selector}: {find_err}")
        return False


    def _try_cookie_banners(self, driver):
        """Try to handle common cookie/consent banners using CSS selectors."""
        consent_selectors = [
            "button#L2AGLb",                      # Google cookie consent (often seen)
            "button[aria-label*='Accept all']",   # More generic accept all
            "button[aria-label*='Agree']",        # More generic agree
            "#onetrust-accept-btn-handler",       # OneTrust banner
            ".cc-banner .cc-btn",                 # Cookieconsent banner
            "button[data-testid='accept-button']",
            "button.tHlp8d",                      # Another Google consent button
            "div.VfPpkd-dgl2Hf-ppHlrf-sM5MNb button", # Material design buttons (might be too broad)
            ".cookie-notice button",
            ".cookie-banner button",
            ".consent-banner button",
            "#cookie-popup button",
            ".gdpr button",
        ]
        for selector in consent_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                for element in elements:
                     if element.is_displayed() and element.is_enabled():
                         try:
                             element.click()
                             self.logger.info(f"Clicked cookie banner button using selector: {selector}")
                             return True
                         except Exception as click_err:
                              self.logger.debug(f"Could not click cookie banner button {selector}: {click_err}")
                              # Try JS click
                              try:
                                   driver.execute_script("arguments[0].click();", element)
                                   self.logger.info(f"Clicked cookie banner button using JS fallback: {selector}")
                                   return True
                              except Exception as js_err:
                                   self.logger.debug(f"JS click failed for cookie banner {selector}: {js_err}")
            except Exception as find_err:
                 self.logger.debug(f"Error finding cookie banner {selector}: {find_err}")
        return False


class GoogleMapsGridScraper:
    """Enhanced Google Maps Grid Scraper with multi-threading and advanced features"""
    def __init__(self, headless=True, max_workers=10, debug=True, cache_enabled=True,
                 proxy_list=None, retry_attempts=3, no_images=False):
        """Initialize the Enhanced Google Maps Grid Scraper"""
        ensure_directories_exist()
        self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.logger, self.grid_logger, self.business_logger = setup_logging(self.session_id)

        self.logger.info(f"🚀 Setting up Enhanced Google Maps Grid Scraper v{VERSION}")
        self.logger.info(f"Session ID: {self.session_id}")

        self.debug = debug
        self.headless = headless
        self.max_workers = max_workers
        self.retry_attempts = retry_attempts
        self.no_images = no_images

        self.browser_pool = BrowserPool(
            max_browsers=max_workers, headless=headless, proxy_list=proxy_list, debug=debug
        )
        self.consent_handler = ConsentHandler(self.logger)
        self.cache = DataCache(enabled=cache_enabled)

        self.debug_dir = self._ensure_dir("debug")
        self.results_dir = self._ensure_dir("results")
        self.temp_dir = self._ensure_dir("temp")
        self.grid_data_dir = self._ensure_dir("grid_data")

        self.results = []
        self.processed_links = set()
        self.seen_businesses = {} # key: (name, address_part), value: index in self.results
        self.grid = []
        self.current_grid_cell = None # Note: Less reliable in parallel mode

        self.lock = threading.Lock() # Lock for shared resources (results, stats, seen_businesses)

        self.stats = defaultdict(int) # Use defaultdict for easier stat updates
        self.stats["start_time"] = None # Keep specific start time

        self.config = {
            "extract_emails": True, "deep_email_search": True, "extract_social": True,
            "save_screenshots": debug, "grid_size_meters": 250, "scroll_attempts": 15,
            "scroll_pause_time": 1.2, "email_timeout": 15, "retry_on_empty": True,
            "expand_grid_areas": True, "max_results": None # Will be set by scrape/resume
        }
        self.logger.info("✅ Initialization complete")

    def _ensure_dir(self, dir_name):
        """Ensure a directory exists and return its path"""
        try:
            dir_path = Path(dir_name)
            dir_path.mkdir(parents=True, exist_ok=True)
            return dir_path
        except Exception as e:
            self.logger.warning(f"Could not create/access directory '{dir_name}': {e}")
            fallback_dir = Path(f"_{dir_name}")
            try:
                fallback_dir.mkdir(parents=True, exist_ok=True)
                self.logger.info(f"Using fallback directory: '{fallback_dir}'")
                return fallback_dir
            except Exception:
                self.logger.error(f"Could not create fallback directory for '{dir_name}'. Using current.")
                return Path(".")

    def get_exact_city_bounds(self, location):
        """Get precise bounding box for a city by finding its extreme points"""
        self.logger.info(f"📍 Finding precise boundaries for location: {location}")
        print(f"Finding precise boundaries for {location}...")
        cache_key = f"bounds_{location}"
        cached_bounds = self.cache.get(cache_key)
        if cached_bounds:
            self.logger.info(f"Using cached bounds for {location}")
            return cached_bounds

        browser_id = None
        try:
            browser_id = self.browser_pool.get_browser()
            driver = self.browser_pool.get_driver(browser_id)
            if not driver: raise Exception("Failed to get driver for bounds check")

            driver.get("https://www.google.com/maps")
            time.sleep(random.uniform(2, 4))
            self.consent_handler.handle_consent(driver, self.debug, self.debug_dir)

            search_box = WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.ID, "searchboxinput"))
            )
            search_box.clear()
            search_box.send_keys(location)
            search_box.send_keys(Keys.ENTER)
            self.logger.info(f"Searched for location: {location}")
            time.sleep(random.uniform(4, 6)) # Allow map to settle

            if self.debug and not self.no_images:
                screenshot_path = self.debug_dir / f"location_search_{self.session_id}.png"
                try: driver.save_screenshot(str(screenshot_path))
                except Exception as e: self.logger.warning(f"Screenshot failed: {e}")

            # Try multiple times to get bounds via JS
            bounds_data = None
            for attempt in range(3):
                 try:
                      bounds_data = driver.execute_script("""
                          try {
                              let mapInstance;
                              // Try finding the map instance associated with a visible map element
                              const mapElement = document.getElementById('map'); // Common ID, adjust if needed
                              if (mapElement && mapElement.__gm) {
                                   mapInstance = mapElement.__gm.map;
                              } else {
                                   // Fallback: Find any element with __gm property
                                   const maps = Array.from(document.querySelectorAll('*')).filter(el => el.__gm && el.__gm.map);
                                   if (maps.length > 0) mapInstance = maps[0].__gm.map;
                              }

                              if (mapInstance && mapInstance.getBounds) {
                                  const bounds = mapInstance.getBounds();
                                  const center = mapInstance.getCenter();
                                  const zoom = mapInstance.getZoom();
                                  if (bounds && center && typeof zoom === 'number') {
                                       return {
                                           northeast: { lat: bounds.getNorthEast().lat(), lng: bounds.getNorthEast().lng() },
                                           southwest: { lat: bounds.getSouthWest().lat(), lng: bounds.getSouthWest().lng() },
                                           center: { lat: center.lat(), lng: center.lng() },
                                           zoom: zoom,
                                           method: 'map-bounds-api'
                                       };
                                  }
                              }
                          } catch (e) { /* Ignore errors during JS execution */ }

                          // Fallback: Extract from URL if API fails
                          const url = window.location.href;
                          const match = url.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+),(\\d+\\.?\\d*)z/);
                          if (match) {
                              const lat = parseFloat(match[1]);
                              const lng = parseFloat(match[2]);
                              const zoom = parseFloat(match[3]);
                              // Estimate bounds based on zoom (adjust factors as needed)
                              const latDelta = 180 / Math.pow(2, zoom); // Rough latitude span
                              const lngDelta = 360 / Math.pow(2, zoom); // Rough longitude span
                              return {
                                  northeast: { lat: lat + latDelta / 2, lng: lng + lngDelta / 2 },
                                  southwest: { lat: lat - latDelta / 2, lng: lng - lngDelta / 2 },
                                  center: { lat: lat, lng: lng },
                                  zoom: zoom,
                                  method: 'url-estimation'
                              };
                          }
                          return null;
                      """)
                      if bounds_data: break # Got data, exit loop
                 except Exception as js_err:
                      self.logger.warning(f"JS bounds extraction attempt {attempt+1} failed: {js_err}")
                 time.sleep(2) # Wait before retrying JS

            if bounds_data:
                self.logger.info(f"Found city bounds: NE={bounds_data['northeast']}, SW={bounds_data['southwest']} (Method: {bounds_data.get('method', 'unknown')})")
                ne, sw = bounds_data['northeast'], bounds_data['southwest']
                lat_delta, lng_delta = abs(ne['lat'] - sw['lat']), abs(ne['lng'] - sw['lng'])
                avg_lat = (ne['lat'] + sw['lat']) / 2
                width_km = lng_delta * 111.32 * math.cos(math.radians(avg_lat))
                height_km = lat_delta * 111.32
                self.logger.info(f"Approximate city size: {width_km:.2f}km x {height_km:.2f}km")
                print(f"City boundaries detected: ~{width_km:.1f}km x {height_km:.1f}km")

                # Expand bounds slightly (e.g., 5-10%)
                expand_factor = 0.05
                center_lat = (ne['lat'] + sw['lat']) / 2
                center_lng = (ne['lng'] + sw['lng']) / 2
                expanded_lat_delta = lat_delta * (1 + expand_factor * 2)
                expanded_lng_delta = lng_delta * (1 + expand_factor * 2)

                expanded_bounds = {
                    'northeast': {'lat': center_lat + expanded_lat_delta / 2, 'lng': center_lng + expanded_lng_delta / 2},
                    'southwest': {'lat': center_lat - expanded_lat_delta / 2, 'lng': center_lng - expanded_lng_delta / 2},
                    'center': {'lat': center_lat, 'lng': center_lng},
                    'width_km': width_km, 'height_km': height_km,
                    'method': bounds_data.get('method', 'unknown')
                }
                self.logger.info(f"Expanded bounds by {expand_factor*100}%: NE={expanded_bounds['northeast']}, SW={expanded_bounds['southwest']}")
                self.cache.set(cache_key, expanded_bounds)
                return expanded_bounds
            else:
                self.logger.error("Could not determine city bounds after multiple attempts.")
                print("❌ Could not determine city boundaries.")
                return None
        except Exception as e:
            self.logger.error(f"Error getting city bounds: {e}", exc_info=True)
            print(f"❌ Error getting city boundaries: {e}")
            if browser_id is not None: self.browser_pool.report_error(browser_id)
            return None # Return None on failure
        finally:
            if browser_id is not None:
                self.browser_pool.release_browser(browser_id)

    def create_optimal_grid(self, bounds, grid_size_meters=250):
        """Create an optimal grid based on city bounds"""
        self.logger.info(f"📊 Creating grid with {grid_size_meters}m cells")
        print(f"Creating grid with {grid_size_meters}m cells...")
        self.grid_logger.debug("=== STARTING GRID CREATION ===")
        # ... (rest of the grid creation logic remains the same) ...
        ne_lat, ne_lng = bounds['northeast']['lat'], bounds['northeast']['lng']
        sw_lat, sw_lng = bounds['southwest']['lat'], bounds['southwest']['lng']

        avg_lat = (ne_lat + sw_lat) / 2
        meters_per_degree_lat = 111132.954 - 559.822 * math.cos(2 * math.radians(avg_lat)) + 1.175 * math.cos(4 * math.radians(avg_lat))
        meters_per_degree_lng = 111319.488 * math.cos(math.radians(avg_lat))

        grid_size_lat = grid_size_meters / meters_per_degree_lat
        grid_size_lng = grid_size_meters / meters_per_degree_lng
        self.grid_logger.debug(f"Grid cell size (degrees): lat={grid_size_lat:.6f}, lng={grid_size_lng:.6f}")

        lat_span = abs(ne_lat - sw_lat)
        lng_span = abs(ne_lng - sw_lng)
        cells_lat = math.ceil(lat_span / grid_size_lat)
        cells_lng = math.ceil(lng_span / grid_size_lng)
        total_cells = cells_lat * cells_lng
        self.grid_logger.debug(f"Grid dimensions: {cells_lat} rows x {cells_lng} columns = {total_cells} total cells")

        if total_cells > 50000: # Add a safety limit
             self.logger.warning(f"Grid size ({total_cells} cells) is very large. Consider increasing grid_size_meters or refining location.")
             print(f"⚠️ Warning: Grid size ({total_cells} cells) is very large.")
             if total_cells > 100000:
                  self.logger.error("Grid size exceeds 100,000 cells. Aborting.")
                  print("❌ Error: Grid size exceeds 100,000 cells. Please use a smaller area or larger grid size.")
                  return None

        grid = []
        for i in range(cells_lat):
            lat1 = sw_lat + (i * grid_size_lat)
            lat2 = lat1 + grid_size_lat
            for j in range(cells_lng):
                lng1 = sw_lng + (j * grid_size_lng)
                lng2 = lng1 + grid_size_lng
                cell = {
                    "southwest": {"lat": lat1, "lng": lng1},
                    "northeast": {"lat": lat2, "lng": lng2},
                    "center": {"lat": (lat1 + lat2) / 2, "lng": (lng1 + lng2) / 2},
                    "row": i, "col": j, "cell_id": f"r{i}c{j}",
                    "likely_empty": False, "processed": False
                }
                grid.append(cell)

        self.logger.info(f"Created grid with {total_cells} cells ({cells_lat}x{cells_lng})")
        print(f"Created grid with {total_cells} cells ({cells_lat} rows x {cells_lng} columns)")
        self.grid_logger.debug("=== GRID CREATION COMPLETE ===")

        # Save grid definition
        try:
            grid_file = self.grid_data_dir / f"grid_definition_{self.session_id}.json"
            with open(grid_file, 'w', encoding='utf-8') as f:
                json.dump(grid, f, indent=2)
            self.logger.info(f"Saved grid definition to {grid_file}")
        except Exception as e:
            self.logger.warning(f"Error saving grid definition: {e}")

        if MATPLOTLIB_AVAILABLE and not self.no_images:
            try: self.generate_grid_visualization(grid, cells_lat, cells_lng)
            except Exception as viz_err: self.logger.warning(f"Grid viz failed: {viz_err}")

        self.grid = grid
        self.stats["grid_cells_total"] = total_cells
        return grid


    def generate_grid_visualization(self, grid, rows, cols):
        """Generate a visual representation of the grid using Matplotlib"""
        # ... (visualization code remains the same) ...
        self.grid_logger.debug("\nGenerating Grid Visualization...")
        try:
            fig, ax = plt.subplots(figsize=(max(10, cols/5), max(8, rows/5)))
            for cell in grid:
                sw, ne = cell["southwest"], cell["northeast"]
                width, height = abs(ne["lng"] - sw["lng"]), abs(ne["lat"] - sw["lat"])
                rect = plt.Rectangle((sw["lng"], sw["lat"]), width, height,
                                     fill=False, edgecolor='blue', linewidth=0.3)
                ax.add_patch(rect)
                # Optionally add cell ID text for smaller grids
                if rows * cols < 500: # Only label small grids
                     ax.text(cell["center"]["lng"], cell["center"]["lat"], cell["cell_id"],
                             ha='center', va='center', fontsize=6, color='gray')

            # Set plot limits based on overall bounds
            min_lng = min(c['southwest']['lng'] for c in grid)
            max_lng = max(c['northeast']['lng'] for c in grid)
            min_lat = min(c['southwest']['lat'] for c in grid)
            max_lat = max(c['northeast']['lat'] for c in grid)
            ax.set_xlim(min_lng, max_lng)
            ax.set_ylim(min_lat, max_lat)

            ax.set_xlabel('Longitude')
            ax.set_ylabel('Latitude')
            ax.set_title(f'Search Grid ({rows}×{cols} cells) - Session {self.session_id}')
            plt.xticks(rotation=45)
            plt.tight_layout()

            grid_viz_path = self.grid_data_dir / f"grid_visualization_{self.session_id}.png"
            plt.savefig(grid_viz_path, dpi=150)
            self.logger.info(f"Saved grid visualization to {grid_viz_path}")
            plt.close(fig)
            self._generate_html_visualization(grid, rows, cols) # Also generate HTML version
        except Exception as e:
            self.logger.error(f"Error creating grid visualization: {e}", exc_info=True)


    def _generate_html_visualization(self, grid, rows, cols):
        """Generate an HTML visualization of the grid"""
        # ... (HTML generation code remains the same) ...
        try:
            html_output = """
            <!DOCTYPE html><html><head><title>Grid Visualization</title><style>
            body { font-family: sans-serif; } .grid { display: table; border-collapse: collapse; margin: 10px; }
            .row { display: table-row; } .cell { display: table-cell; border: 1px solid #ccc;
            min-width: 40px; height: 20px; text-align: center; vertical-align: middle; font-size: 9px; padding: 1px;}
            .empty { background-color: #f0f0f0; } .processed { background-color: #e0ffe0; }
            .current { background-color: #fff0b3; } .header { font-weight: bold; background-color: #eee; }
            .legend { margin: 10px; padding: 5px; border: 1px solid #ccc; display: inline-block; font-size: 12px;}
            .legend-item { margin: 3px; } .legend-box { display: inline-block; width: 15px; height: 15px; margin-right: 5px; vertical-align: middle; border: 1px solid #aaa;}
            </style></head><body><h1>Grid Visualization</h1><div class="legend">Legend:
            <div class="legend-item"><div class="legend-box" style="background-color: white;"></div> Not Processed</div>
            <div class="legend-item"><div class="legend-box processed" style="background-color: #e0ffe0;"></div> Processed</div>
            <div class="legend-item"><div class="legend-box empty" style="background-color: #f0f0f0;"></div> Processed (Empty)</div>
            </div><div class="grid">
            """
            # Headers
            html_output += "<div class='row'><div class='cell header'>R\\C</div>"
            for c in range(cols): html_output += f"<div class='cell header'>{c}</div>"
            html_output += "</div>"
            # Rows
            for r in range(rows):
                html_output += f"<div class='row'><div class='cell header'>{r}</div>"
                for c in range(cols):
                    cell_id = f"r{r}c{c}"
                    # Find cell status (less efficient but ok for viz)
                    cell_data = next((cell for cell in grid if cell["cell_id"] == cell_id), None)
                    cell_class = "cell"
                    if cell_data:
                         if cell_data.get("processed"):
                              cell_class += " processed"
                              if cell_data.get("likely_empty"):
                                   cell_class += " empty"
                    html_output += f"<div class='{cell_class}' title='{cell_id}'>{cell_id}</div>"
                html_output += "</div>"

            html_output += "</div></body></html>"
            html_viz_path = self.grid_data_dir / f"grid_visualization_{self.session_id}.html"
            with open(html_viz_path, "w", encoding='utf-8') as f: f.write(html_output)
            self.logger.info(f"Saved HTML grid visualization to {html_viz_path}")
        except Exception as e:
            self.logger.warning(f"Error creating HTML visualization: {e}")


    def update_grid_visualization(self):
        """Update the HTML grid visualization with current progress"""
        if not self.grid: return # or not self.grid_data_dir.exists(): return
        try:
            rows = max(cell["row"] for cell in self.grid) + 1
            cols = max(cell["col"] for cell in self.grid) + 1

            # Create status map (thread-safe read of self.grid)
            with self.lock: # Lock grid access briefly
                 grid_copy = list(self.grid) # Work on a copy

            cell_status = {cell["cell_id"]: cell for cell in grid_copy}

            # Calculate progress percentage safely
            total_cells = self.stats["grid_cells_total"]
            processed_cells = self.stats["grid_cells_processed"]
            progress_percent = int(100 * processed_cells / max(1, total_cells))


            html_output = f"""
            <!DOCTYPE html><html><head><meta http-equiv="refresh" content="30"><title>Grid Progress</title><style>
            body {{ font-family: sans-serif; }} .grid {{ display: table; border-collapse: collapse; margin: 10px; }}
            .row {{ display: table-row; }} .cell {{ display: table-cell; border: 1px solid #ccc;
            min-width: 40px; height: 20px; text-align: center; vertical-align: middle; font-size: 9px; padding: 1px;}}
            .empty {{ background-color: #f0f0f0 !important; }} .processed {{ background-color: #e0ffe0; }}
            .header {{ font-weight: bold; background-color: #eee; }}
            .stats, .legend {{ margin: 10px; padding: 10px; border: 1px solid #ccc; font-size: 14px; }}
            .legend-item {{ margin: 3px; }} .legend-box {{ display: inline-block; width: 15px; height: 15px; margin-right: 5px; vertical-align: middle; border: 1px solid #aaa;}}
            .progress-bar-container {{ width: 90%; background-color: #f0f0f0; border-radius: 4px; margin: 10px 0; }}
            .progress-bar {{ height: 20px; background-color: #4CAF50; border-radius: 4px; text-align: center; color: white; line-height: 20px; }}
            </style></head><body><h1>Grid Progress</h1><div class="stats"><h2>Scraping Statistics</h2>
            <p>Total Cells: {total_cells}</p><p>Processed Cells: {processed_cells}</p>
            <div class="progress-bar-container"><div class="progress-bar" style="width: {progress_percent}%">{progress_percent}%</div></div>
            <p>Empty Cells Found: {self.stats["grid_cells_empty"]}</p><p>Businesses Found: {self.stats["businesses_found"]}</p>
            <p>Emails Found: {self.stats["email_found_count"]}</p><p>Time Elapsed: {self.get_elapsed_time()}</p>
            </div><div class="legend">Legend:
            <div class="legend-item"><div class="legend-box" style="background-color: white;"></div> Not Processed</div>
            <div class="legend-item"><div class="legend-box processed" style="background-color: #e0ffe0;"></div> Processed</div>
            <div class="legend-item"><div class="legend-box empty" style="background-color: #f0f0f0;"></div> Processed (Empty)</div>
            </div><div class="grid">
            """
            # Headers
            html_output += "<div class='row'><div class='cell header'>R\\C</div>"
            for c in range(cols): html_output += f"<div class='cell header'>{c}</div>"
            html_output += "</div>"
            # Rows
            for r in range(rows):
                html_output += f"<div class='row'><div class='cell header'>{r}</div>"
                for c in range(cols):
                    cell_id = f"r{r}c{c}"
                    cell_data = cell_status.get(cell_id)
                    cell_class = "cell"
                    if cell_data:
                         if cell_data.get("processed"):
                              cell_class += " processed"
                              if cell_data.get("likely_empty"):
                                   cell_class += " empty"
                    html_output += f"<div class='{cell_class}' title='{cell_id}'>{cell_id}</div>"
                html_output += "</div>"

            html_output += "</div></body></html>"

            progress_path = self.grid_data_dir / f"grid_progress_{self.session_id}.html"
            with open(progress_path, "w", encoding='utf-8') as f: f.write(html_output)
        except Exception as e:
            self.logger.warning(f"Error updating grid visualization: {e}", exc_info=True)


    def get_elapsed_time(self):
        """Get elapsed time in human-readable format"""
        if not self.stats["start_time"]: return "00:00:00"
        elapsed_seconds = (datetime.now() - self.stats["start_time"]).total_seconds()
        hours, remainder = divmod(int(elapsed_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def search_in_grid_cell(self, query, grid_cell):
        """Search for places in a specific grid cell. Returns list of links. Marks cell as processed."""
        cell_id = grid_cell["cell_id"]
        center = grid_cell["center"]
        thread_id = threading.get_ident() # Identify thread for logging
        self.logger.debug(f"Thread {thread_id} starting search in grid cell {cell_id}")

        browser_id = None # Initialize browser_id
        try:
            browser_id = self.browser_pool.get_browser()
            driver = self.browser_pool.get_driver(browser_id)
            if not driver:
                raise Exception(f"Failed to get driver for cell {cell_id}")

            # Construct search URL
            # Use higher zoom level (e.g., 18z) for smaller grid cells to focus search
            zoom_level = 18 if self.config.get("grid_size_meters", 250) <= 300 else 17
            url = f"https://www.google.com/maps/search/{quote(query)}/@{center['lat']:.7f},{center['lng']:.7f},{zoom_level}z"
            self.logger.info(f"Thread {thread_id} - Cell {cell_id} URL: {url}")

            driver.get(url)
            time.sleep(random.uniform(2, 4)) # Wait for initial load

            # Handle consent/login immediately after loading
            if self.consent_handler.handle_consent(driver, self.debug, self.debug_dir):
                 self.stats["consent_pages_handled"] += 1
                 time.sleep(random.uniform(1, 2)) # Extra wait after consent handling

            # Check if redirected (e.g., to consent/login again or error page)
            if "google.com/maps/search" not in driver.current_url:
                 self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Redirected from search results page to: {driver.current_url}. Skipping cell.")
                 # Mark cell as processed but likely problematic, not necessarily empty
                 with self.lock:
                      grid_cell["processed"] = True
                      self.stats["grid_cells_processed"] += 1
                      self.stats["extraction_errors"] += 1 # Count as an error
                 return [] # Return empty list

            # Wait for results feed to appear
            try:
                WebDriverWait(driver, 15).until( # Increased wait
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed']"))
                )
                self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Results feed loaded.")
            except TimeoutException:
                # Check for "No results found" message
                no_results_elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'No results found')] | //*[contains(text(), 'Aucun résultat')] | //*[contains(text(), 'Keine Ergebnisse')]") # Add other languages if needed
                if no_results_elements:
                     self.logger.info(f"Thread {thread_id} - Cell {cell_id} - Explicitly found 'No results found'.")
                     with self.lock:
                          grid_cell["processed"] = True
                          grid_cell["likely_empty"] = True
                          self.stats["grid_cells_processed"] += 1
                          self.stats["grid_cells_empty"] += 1
                     return []
                else:
                     self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Timeout waiting for search results feed, but no 'No results' message found.")
                     # Proceed cautiously, might be a slow load or different layout
                     time.sleep(3)


            # Take screenshot if debugging
            if self.debug and not self.no_images and random.random() < 0.1: # Screenshot 10% of cells
                screenshot_path = self.debug_dir / f"grid_cell_{cell_id}_{self.session_id}.png"
                try: driver.save_screenshot(str(screenshot_path))
                except Exception as e: self.logger.warning(f"Screenshot failed for {cell_id}: {e}")

            # --- Link Extraction ---
            business_links = set() # Use a set for automatic deduplication

            # Extract visible links first
            try:
                 visible_links = self.extract_visible_links(driver)
                 if visible_links: business_links.update(visible_links)
                 self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Found {len(business_links)} initial links.")
            except Exception as e:
                 self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Error extracting initial links: {e}")


            # Scroll and collect more links
            try:
                 scroll_links = self.scroll_and_collect_links(driver, max_scrolls=self.config["scroll_attempts"])
                 if scroll_links: business_links.update(scroll_links)
                 self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Found {len(business_links)} total links after scrolling.")
            except Exception as e:
                 self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Error scrolling/collecting links: {e}")

            business_links_list = list(business_links)

            # --- Update Cell Status and Stats ---
            with self.lock: # Lock for updating shared grid state and stats
                 if not grid_cell.get("processed"): # Ensure we only count processing once
                     self.stats["grid_cells_processed"] += 1
                 grid_cell["processed"] = True

                 if not business_links_list:
                     self.logger.info(f"Thread {thread_id} - Cell {cell_id} - No business links found.")
                     grid_cell["likely_empty"] = True
                     self.stats["grid_cells_empty"] += 1 # Increment only if it wasn't already marked empty
                 else:
                     self.logger.info(f"Thread {thread_id} - Cell {cell_id} - Found {len(business_links_list)} unique business links.")
                     grid_cell["likely_empty"] = False # Mark as not empty if links found

            # Save links to temp file (keep for recovery)
            if business_links_list:
                try:
                    links_file = self.temp_dir / f"cell_{cell_id}_links_{self.session_id}.json"
                    with open(links_file, "w", encoding='utf-8') as f:
                        json.dump(business_links_list, f)
                except Exception as e:
                    self.logger.warning(f"Error saving links temp file for {cell_id}: {e}")

            return business_links_list

        except Exception as e:
            self.logger.error(f"Thread {thread_id} - Error searching in grid cell {cell_id}: {e}", exc_info=True)
            if browser_id is not None: self.browser_pool.report_error(browser_id)
            # Mark cell as processed with error
            with self.lock:
                if not grid_cell.get("processed"):
                     self.stats["grid_cells_processed"] += 1
                grid_cell["processed"] = True
                self.stats["extraction_errors"] += 1 # Count as error
            return [] # Return empty list on failure
        finally:
            if browser_id is not None:
                self.browser_pool.release_browser(browser_id)


    def extract_visible_links(self, driver):
        """Extract visible business links without scrolling"""
        # ... (JS extraction logic remains largely the same) ...
        try:
            links = driver.execute_script("""
                const links = new Set();
                // Selector targets links within result items more specifically
                document.querySelectorAll('div[role="feed"] a[href*="/maps/place/"], div.Nv2PK a[href*="/maps/place/"], div.bfdHYd a[href*="/maps/place/"]').forEach(el => {
                     // Basic validation of the URL structure
                     if (el.href && el.href.includes('/maps/place/') && el.href.includes('/@')) {
                          links.add(el.href);
                     }
                });
                return Array.from(links);
            """)
            return links if links else []
        except Exception as e:
            self.logger.warning(f"Error extracting visible links: {e}")
            return []


    def scroll_and_collect_links(self, driver, max_scrolls=15):
        """Scroll through results and collect business links"""
        # ... (Scrolling logic remains largely the same, ensure JS link extraction is robust) ...
        links_found = set()
        stagnant_count = 0
        scroll_element = None

        # Try finding the scrollable feed first
        selectors = ["div[role='feed']", "div.m6QErb > div[aria-label]", "div.DxyBCb"]
        for selector in selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                # Find the element with the largest scrollHeight, likely the main feed
                best_element = None
                max_scroll = -1
                for el in elements:
                     sh = driver.execute_script("return arguments[0].scrollHeight", el)
                     if sh > max_scroll:
                          max_scroll = sh
                          best_element = el
                if best_element:
                     scroll_element = best_element
                     self.logger.debug(f"Found scrollable container with selector: {selector} (scrollHeight: {max_scroll})")
                     break
            except Exception: continue

        if not scroll_element:
            self.logger.warning("Could not find specific scrollable feed, falling back to scrolling window/body.")
            # Fallback: scroll window
            scroll_element = driver.find_element(By.TAG_NAME, "body") # Or use None and scroll window directly

        last_scroll_height = 0
        for i in range(max_scrolls):
            initial_link_count = len(links_found)

            # Scroll down
            try:
                 if scroll_element and scroll_element.tag_name != "body":
                      current_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element)
                      driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_element)
                 else: # Scroll window
                      current_scroll_height = driver.execute_script("return document.body.scrollHeight")
                      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

                 time.sleep(random.uniform(self.config["scroll_pause_time"], self.config["scroll_pause_time"] + 0.5)) # Wait for load

                 # Check if scroll height changed significantly
                 if abs(current_scroll_height - last_scroll_height) < 50 and i > 0: # If height didn't change much
                      stagnant_count += 1
                      self.logger.debug(f"Scroll height stagnant ({stagnant_count}) at iteration {i+1}")
                 else:
                      stagnant_count = 0 # Reset if height changed
                 last_scroll_height = current_scroll_height

            except Exception as scroll_err:
                 self.logger.warning(f"Error during scroll: {scroll_err}")
                 stagnant_count += 1 # Count as stagnant if scroll fails

            # Extract links after scrolling
            try:
                new_links = driver.execute_script("""
                    const links = new Set();
                    document.querySelectorAll('div[role="feed"] a[href*="/maps/place/"], div.Nv2PK a[href*="/maps/place/"], div.bfdHYd a[href*="/maps/place/"]').forEach(el => {
                         if (el.href && el.href.includes('/maps/place/') && el.href.includes('/@')) {
                              links.add(el.href);
                         }
                    });
                    return Array.from(links);
                """)
                if new_links: links_found.update(new_links)
            except Exception as extract_err:
                 self.logger.warning(f"Error extracting links after scroll {i+1}: {extract_err}")


            # Check for "end of results" message more reliably
            end_markers = driver.find_elements(By.XPATH, "//*[contains(text(), \"You've reached the end of the list\")] | //*[contains(text(), \"Vous êtes arrivé au bout de la liste\")] | //*[contains(text(), \"Sie haben das Ende der Liste erreicht\")]") # Add more languages
            if end_markers:
                 self.logger.info(f"Reached end of results marker after scroll {i+1}.")
                 break

            # Break if stagnant for too long
            if stagnant_count >= 3:
                self.logger.info(f"Scrolling stopped after {i+1} scrolls due to stagnant content/scroll height.")
                break

        return list(links_found)


    def extract_place_info(self, url, driver):
        """Extract business information from a Google Maps URL"""
        thread_id = threading.get_ident() # Identify thread for logging
        # Check processed links (read is generally safe without lock, but add uses lock)
        if url in self.processed_links:
            self.logger.debug(f"Thread {thread_id} - Skipping already processed URL: {url[:50]}...")
            return None

        # Basic URL validation
        if not url or not url.startswith("http") or "google.com/maps/place/" not in url:
             self.logger.warning(f"Thread {thread_id} - Skipping invalid URL: {url}")
             return None

        # Check for rate limit / consent pages (already handled by search_in_grid_cell, but double check)
        if "sorry/index" in url or "consent" in url or "batchexecute" in url:
            with self.lock: self.stats["rate_limit_hits"] += 1
            self.logger.warning(f"Thread {thread_id} - Skipping likely rate limit/consent URL: {url[:50]}...")
            return None

        self.logger.debug(f"Thread {thread_id} - Processing URL: {url[:80]}...")

        place_info = defaultdict(str) # Use defaultdict for easier assignments
        place_info.update({
            "maps_url": url,
            "social_links": {},
            "scrape_date": datetime.now().strftime('%Y-%m-%d'),
            "place_id": self.extract_place_id(url)
            # grid_cell will be added by the calling function (process_grid_cell)
        })

        try:
            # Load the business page
            driver.get(url)
            time.sleep(random.uniform(2.5, 4.0)) # Wait for page elements to render

            # Handle consent/login again if it appears on the place page
            if self.consent_handler.handle_consent(driver, self.debug, self.debug_dir):
                 self.stats["consent_pages_handled"] += 1
                 time.sleep(random.uniform(1, 2))

            # Check for rate limit / redirection after load
            current_page_url = driver.current_url
            if "sorry/index" in current_page_url or "consent" in current_page_url or "batchexecute" in current_page_url:
                with self.lock: self.stats["rate_limit_hits"] += 1
                self.logger.warning(f"Thread {thread_id} - Hit rate limit/consent page loading place: {url[:50]}...")
                return None
            if "google.com/maps/search" in current_page_url: # If redirected back to search
                 self.logger.warning(f"Thread {thread_id} - Redirected back to search page from place URL: {url[:50]}...")
                 return None


            # --- Extract Core Information ---
            # Prioritize JS extraction as it's often more reliable if elements are found
            try:
                 js_data = driver.execute_script(self._get_js_extraction_script())
                 if js_data:
                      for key, value in js_data.items():
                           if value: # Only update if JS found something
                                place_info[key] = value
                      self.logger.debug(f"Thread {thread_id} - JS extracted data for {url[:50]}: {js_data}")
            except Exception as js_err:
                 self.logger.warning(f"Thread {thread_id} - JS extraction failed for {url[:50]}: {js_err}")


            # --- Fallback/Supplement with Selenium Finders ---
            # Name (Crucial - try multiple selectors)
            if not place_info["name"]:
                 name_selectors = ["h1", "h1[class*='headline']", "h1[class*='header']", "[role='main'] h1"]
                 for sel in name_selectors:
                      try:
                           name_el = driver.find_element(By.CSS_SELECTOR, sel)
                           place_info["name"] = name_el.text.strip()
                           if place_info["name"]: break
                      except NoSuchElementException: continue
                      except Exception as e: self.logger.debug(f"Name selector {sel} error: {e}")

            # If still no name, it's likely a failed load or weird page
            if not place_info["name"]:
                self.logger.warning(f"Thread {thread_id} - Could not extract name for URL: {url[:80]}... Skipping.")
                with self.lock: self.stats["extraction_errors"] += 1
                return None

            # Address
            if not place_info["address"]:
                 try:
                      # Look for button with address icon/tooltip
                      addr_el = driver.find_element(By.CSS_SELECTOR, "button[data-item-id^='address'] div:last-child")
                      place_info["address"] = addr_el.text.strip()
                 except Exception:
                      try: # Fallback to aria-label
                           addr_el = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Address:']")
                           place_info["address"] = addr_el.get_attribute('aria-label').replace("Address:", "").strip()
                      except Exception as e: self.logger.debug(f"Address extraction failed: {e}")


            # Phone
            if not place_info["phone"]:
                 try:
                      phone_el = driver.find_element(By.CSS_SELECTOR, "button[data-item-id^='phone:tel:'] div:last-child")
                      place_info["phone"] = phone_el.text.strip()
                 except Exception:
                      try:
                           phone_el = driver.find_element(By.CSS_SELECTOR, "button[aria-label*='Phone:']")
                           place_info["phone"] = phone_el.get_attribute('aria-label').replace("Phone:", "").strip()
                      except Exception as e: self.logger.debug(f"Phone extraction failed: {e}")


            # Website
            if not place_info["website"]:
                 try:
                      web_el = driver.find_element(By.CSS_SELECTOR, "a[data-item-id='authority']")
                      place_info["website"] = web_el.get_attribute('href')
                 except Exception:
                      try:
                           web_el = driver.find_element(By.CSS_SELECTOR, "a[aria-label*='Website:']")
                           place_info["website"] = web_el.get_attribute('href')
                      except Exception as e: self.logger.debug(f"Website extraction failed: {e}")


            # Category (often near rating)
            if not place_info["category"]:
                 try:
                      # Look for button next to rating/reviews
                      cat_el = driver.find_element(By.CSS_SELECTOR, "button[jsaction*='category']")
                      place_info["category"] = cat_el.text.strip()
                 except Exception as e: self.logger.debug(f"Category extraction failed: {e}")


            # Rating & Reviews (often together)
            if not place_info["rating"] or not place_info["reviews_count"]:
                 try:
                      # Common pattern: Span with rating, span with num reviews in parentheses
                      rating_area = driver.find_element(By.CSS_SELECTOR, "div.F7nice") # Container div
                      rating_span = rating_area.find_element(By.CSS_SELECTOR, "span[aria-hidden='true']") # The number itself
                      review_span = rating_area.find_element(By.CSS_SELECTOR, "span[aria-label*='reviews']") # Span like "(1,234)"
                      place_info["rating"] = rating_span.text.strip()
                      place_info["reviews_count"] = review_span.text.strip().replace('(','').replace(')','').replace(',','')
                 except Exception as e: self.logger.debug(f"Rating/Review extraction failed: {e}")


            # --- Additional Extractions ---
            # Coordinates (re-extract from current URL in case it updated)
            place_info["coordinates"] = self.extract_coordinates_from_url(driver.current_url)

            # Social Media Links
            if self.config["extract_social"]:
                try: place_info["social_links"] = self.extract_social_media_links(driver)
                except Exception as e: self.logger.warning(f"Social link extraction failed: {e}")

            # Email (only if website found and enabled)
            if place_info["website"] and self.config["extract_emails"]:
                 # Use a separate browser instance for email extraction to isolate potential issues
                 email_browser_id = None
                 try:
                      email_browser_id = self.browser_pool.get_browser(timeout=15) # Shorter timeout for email
                      email_driver = self.browser_pool.get_driver(email_browser_id)
                      if email_driver:
                           email = self._extract_email_from_site(place_info["website"], email_driver)
                           if email:
                                place_info["email"] = email
                                with self.lock: self.stats["email_found_count"] += 1
                 except TimeoutError:
                      self.logger.warning(f"Timeout getting browser for email extraction for {place_info['website']}")
                 except Exception as email_err:
                      self.logger.warning(f"Email extraction failed for {place_info['website']}: {email_err}")
                      if email_browser_id is not None: self.browser_pool.report_error(email_browser_id)
                 finally:
                      if email_browser_id is not None: self.browser_pool.release_browser(email_browser_id)


            # --- Final Steps ---
            # Log success and update stats
            self.logger.info(f"Thread {thread_id} - Successfully extracted: {place_info['name']}")
            with self.lock:
                 self.stats["successful_extractions"] += 1
                 self.processed_links.add(url) # Add to processed only on success

            # Log business details
            business_log = {k: v for k, v in place_info.items() if k != 'social_links'} # Exclude dict
            if place_info.get("social_links"):
                 business_log.update(place_info["social_links"]) # Flatten social links
            self.business_logger.info(json.dumps(business_log))

            return dict(place_info) # Convert back to regular dict

        except Exception as e:
            self.logger.error(f"Thread {thread_id} - Error extracting place info for {url[:80]}: {e}", exc_info=True)
            with self.lock: self.stats["extraction_errors"] += 1
            # Don't add to processed_links on error
            return None


    def _get_js_extraction_script(self):
         """Returns the JavaScript code string for extracting business info."""
         # This keeps the main extract_place_info cleaner
         return """
            function extractBusinessInfo() {
                const data = { name: "", address: "", phone: "", website: "", rating: "", reviews_count: "", category: "", hours: "", price_level: "" };
                const getText = (selector, attribute = 'textContent') => {
                    const el = document.querySelector(selector);
                    if (!el) return "";
                    const value = attribute === 'textContent' ? el.textContent : el.getAttribute(attribute);
                    return value ? value.trim() : "";
                };
                const getTextFromMultiple = (selectors, attribute = 'textContent') => {
                     for (const selector of selectors) {
                          const text = getText(selector, attribute);
                          if (text) return text;
                     }
                     return "";
                };

                // Name (try h1 first)
                data.name = getText('h1');

                // Address (look for button with address icon)
                data.address = getText('button[data-item-id^="address"] div:last-child') || getTextFromMultiple(['button[aria-label*="Address:"]', 'button[aria-label*="Adresse:"]'], 'aria-label').replace(/Address:|Adresse:/gi, '').trim();

                // Phone (look for button with phone icon)
                data.phone = getText('button[data-item-id^="phone:tel:"] div:last-child') || getTextFromMultiple(['button[aria-label*="Phone:"]', 'button[aria-label*="Telefon:"]'], 'aria-label').replace(/Phone:|Telefon:/gi, '').trim();

                // Website (look for authority link or website icon link)
                data.website = getText('a[data-item-id="authority"]', 'href') || getTextFromMultiple(['a[aria-label*="Website:"]', 'a[aria-label*="Site Web:"]'], 'href');

                // Rating & Reviews (common structure)
                try {
                    const ratingEl = document.querySelector('div.F7nice'); // Common container
                    if (ratingEl) {
                        const ratingVal = ratingEl.querySelector('span[aria-hidden="true"]');
                        const reviewCountSpan = ratingEl.querySelector('span[aria-label*="reviews"], span[aria-label*="avis"], span[aria-label*="Bewertungen"]'); // Add languages
                        if (ratingVal) data.rating = ratingVal.textContent.trim();
                        if (reviewCountSpan) data.reviews_count = reviewCountSpan.textContent.trim().replace(/[^0-9,]/g, '').replace(',', ''); // Extract numbers only
                    }
                } catch (e) {}

                // Category (button near rating)
                data.category = getText('button[jsaction*="category"]');

                // Price Level (span with $ signs)
                data.price_level = getText('span[aria-label*="Price"]'); // Might be like "$$ · Category"

                // Hours (more complex, might need specific selectors if JS needed)
                // data.hours = getText('div[jsaction*="openhours"]'); // Example

                return data;
            }
            return extractBusinessInfo();
         """

    def extract_place_id(self, url):
        """Extract place ID from Google Maps URL"""
        # ... (remains the same) ...
        try:
            place_id_match = re.search(r'!1s([a-zA-Z0-9:_-]+)(?:!|$)', url) # Look for !1s followed by ID and ! or end
            if place_id_match: return place_id_match.group(1)
            alt_match = re.search(r'data=.*!1s([a-zA-Z0-9:_-]+)', url) # Alternative within data param
            if alt_match: return alt_match.group(1)
            parsed_url = urlparse(url)
            path_parts = parsed_url.path.split('/')
            # Example path: /maps/place/Business+Name/data=!4m2!3m1!1s0x.....
            if 'place' in path_parts:
                 data_index = -1
                 for i, part in enumerate(path_parts):
                      if part.startswith('data='):
                           data_index = i
                           break
                 if data_index > 0:
                      data_part = path_parts[data_index]
                      id_match_in_data = re.search(r'!1s([a-zA-Z0-9:_-]+)', data_part)
                      if id_match_in_data: return id_match_in_data.group(1)

            query_params = parse_qs(parsed_url.query)
            if 'place_id' in query_params: return query_params['place_id'][0]
            return ""
        except Exception: return ""


    def extract_coordinates_from_url(self, url):
        """Extract coordinates from a Google Maps URL"""
        # ... (remains the same) ...
        try:
            coords_match = re.search(r'@(-?\d+\.\d{4,}),(-?\d+\.\d{4,})', url) # Require at least 4 decimal places
            if coords_match:
                lat, lng = coords_match.group(1), coords_match.group(2)
                return f"{lat},{lng}"
            return ""
        except Exception: return ""


    def extract_social_media_links(self, driver):
        """Extract social media links from a business page using JS"""
        # ... (JS extraction remains the same) ...
        try:
            social_links = driver.execute_script("""
                const socialLinks = {};
                const socialDomains = {
                    'facebook.com': 'facebook', 'fb.com': 'facebook', 'instagram.com': 'instagram',
                    'twitter.com': 'twitter', 'x.com': 'twitter', 'linkedin.com': 'linkedin',
                    'youtube.com': 'youtube', 'pinterest.com': 'pinterest', 'tiktok.com': 'tiktok',
                    'yelp.com': 'yelp' // Add others if needed
                };
                document.querySelectorAll('a[href]').forEach(link => {
                    const href = link.href;
                    if (!href) return;
                    try {
                         const url = new URL(href);
                         const domain = url.hostname.replace(/^www\./, ''); // Remove www.
                         for (const [socialDomain, network] of Object.entries(socialDomains)) {
                              if (domain.includes(socialDomain)) {
                                   // Avoid login/share links
                                   if (!href.includes('/sharer') && !href.includes('/intent') && !href.includes('login') && !href.includes('signup')) {
                                        socialLinks[network] = href; // Store the first found link per network
                                        break;
                                   }
                              }
                         }
                    } catch (e) { /* Ignore invalid URLs */ }
                });
                return socialLinks;
            """)
            if social_links: self.logger.debug(f"Found social links: {social_links}")
            return social_links
        except Exception as e:
            self.logger.warning(f"Error extracting social media links: {e}")
            return {}


    def _extract_email_from_site(self, website_url, driver):
         """Internal method to extract email using a provided driver instance."""
         # This assumes the driver is ready and obtained from the pool by the caller
         self.logger.info(f"Attempting email extraction from: {website_url}")
         try:
              driver.get(website_url)
              time.sleep(random.uniform(2, 3)) # Wait for basic load

              # Execute JS to find emails (improved regex and filtering)
              emails = driver.execute_script("""
                  const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
                  const pageText = document.body.innerText || '';
                  const pageSource = document.documentElement.outerHTML || '';
                  let foundEmails = new Set();

                  // Find in visible text and source
                  (pageText.match(emailRegex) || []).forEach(e => foundEmails.add(e));
                  (pageSource.match(emailRegex) || []).forEach(e => foundEmails.add(e));

                  // Find in mailto links
                  document.querySelectorAll('a[href^="mailto:"]').forEach(link => {
                      try {
                           const email = new URL(link.href).pathname;
                           if (email && email.includes('@')) foundEmails.add(email);
                      } catch(e){}
                  });

                  // Filter out common invalid/placeholder emails and image extensions
                  const invalidPatterns = /example|placeholder|yourdomain|domain\.com|sentry|png|jpg|jpeg|gif|webp|svg/i;
                  const validEmails = Array.from(foundEmails).filter(email =>
                       !invalidPatterns.test(email) && email.includes('.') // Basic TLD check
                  );

                  // Prioritize emails (e.g., info@, contact@)
                  const priorityPrefixes = ['info@', 'contact@', 'support@', 'sales@', 'hello@', 'office@'];
                  let primaryEmail = '';
                  for (const prefix of priorityPrefixes) {
                       primaryEmail = validEmails.find(e => e.toLowerCase().startsWith(prefix));
                       if (primaryEmail) break;
                  }

                  return primaryEmail || (validEmails.length > 0 ? validEmails[0] : ''); // Return priority or first valid
              """)

              if emails:
                  self.logger.info(f"Found email on {website_url}: {emails}")
                  return emails
              else:
                  self.logger.info(f"No valid email found on {website_url}")
                  return ""

         except TimeoutException:
              self.logger.warning(f"Timeout loading website for email extraction: {website_url}")
              return ""
         except Exception as e:
              # Log specific webdriver errors differently if needed
              self.logger.warning(f"Error during email extraction from {website_url}: {type(e).__name__} - {e}")
              return ""
         # Note: Browser release is handled by the caller (extract_place_info)


    # --- Main Scraping Logic ---
    def scrape(self, query, location, grid_size_meters=250, max_results=None):
        """Main method to scrape businesses using the enhanced grid approach with parallelism"""
        self.stats["start_time"] = datetime.now()
        start_time = self.stats["start_time"]
        self.config["max_results"] = max_results # Store for access in threads if needed

        self.logger.info(f"🚀 STARTING ENHANCED GRID SCRAPING - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Query: '{query}' in '{location}'")
        self.logger.info(f"Grid size: {grid_size_meters} meters")
        self.logger.info(f"Max results: {max_results or 'unlimited'}")
        self.logger.info(f"Using {self.max_workers} workers")

        print("\n===== STARTING ENHANCED GOOGLE MAPS GRID SCRAPER =====")
        print(f"Search: '{query}' in '{location}'")
        print(f"Grid size: {grid_size_meters} meters")
        print(f"Max results: {max_results or 'unlimited'}")
        print(f"Max workers: {self.max_workers}")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Session ID: {self.session_id}")
        print("=======================================================\n")

        try:
            bounds = self.get_exact_city_bounds(location)
            if not bounds: return []

            grid = self.create_optimal_grid(bounds, grid_size_meters)
            if not grid: return []

            grid = self.sort_grid_cells_by_density(grid)
            total_cells = len(grid)
            self.stats["grid_cells_total"] = total_cells

            print(f"\nProcessing {total_cells} grid cells using up to {self.max_workers} workers...")

            futures = []
            processed_cells_count = 0
            initial_results_count = len(self.results)
            stop_submission = False # Flag to stop submitting new tasks

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='GridWorker') as executor:
                # Submit initial batch of tasks
                tasks_to_submit = list(grid) # Create a list to iterate over

                # Use tqdm for progress bar
                with tqdm(total=total_cells, desc="Processing Grid Cells", unit="cell", smoothing=0.1) as progress_bar:
                    for cell in tasks_to_submit:
                        # Check BEFORE submitting if max_results is reached
                        with self.lock: current_results_count = len(self.results)
                        if max_results and current_results_count >= max_results:
                            if not stop_submission: # Log only once
                                 self.logger.info(f"Max results ({max_results}) reached. Stopping submission of new cell tasks.")
                                 print(f"\nMax results ({max_results}) reached, waiting for running tasks to complete...")
                                 stop_submission = True
                            # Don't break here, let already submitted tasks finish
                            # break # Use this if you want to hard stop immediately

                        if not stop_submission:
                             futures.append(executor.submit(self.process_grid_cell, query, cell))
                        else:
                             # If stopping submission, update progress bar for skipped cells
                             progress_bar.update(1)


                    # Process completed futures
                    for future in concurrent.futures.as_completed(futures):
                        processed_cells_count += 1
                        try:
                            processed_cell = future.result() # process_grid_cell returns the cell dict
                            if processed_cell:
                                # Update the master grid list (optional, mainly for visualization)
                                # Find and update the cell in self.grid based on cell_id
                                with self.lock: # Lock if modifying self.grid directly
                                     for idx, c in enumerate(self.grid):
                                          if c['cell_id'] == processed_cell['cell_id']:
                                               self.grid[idx] = processed_cell
                                               break
                        except Exception as exc:
                            self.logger.error(f'A grid cell task generated an exception: {exc}', exc_info=True)

                        # Update progress bar for completed/failed task
                        progress_bar.update(1)

                        # Update visualization periodically
                        if processed_cells_count % 20 == 0 or processed_cells_count == total_cells:
                             self.update_grid_visualization()

                        # Check max_results again after processing (redundant if checked before submit, but safe)
                        # with self.lock: current_results_count = len(self.results)
                        # if max_results and current_results_count >= max_results:
                        #     # Cancellation logic here if needed, but stopping submission is usually enough
                        #     pass

            # --- End of parallel processing ---
            self.logger.info("All submitted tasks completed.")
            self.save_results() # Final save
            self.generate_statistics_report()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60
            final_results_count = len(self.results)
            new_businesses_found = final_results_count - initial_results_count
            unique_businesses = len(set((r["name"], r.get("address", "")) for r in self.results))

            self.logger.info(f"✅ GRID SCRAPING COMPLETE")
            self.logger.info(f"Found {final_results_count} total businesses ({unique_businesses} unique)")
            self.logger.info(f"Added {new_businesses_found} new businesses this run.")
            self.logger.info(f"Processed {self.stats['grid_cells_processed']} / {total_cells} grid cells")
            self.logger.info(f"Duration: {duration:.2f} minutes")

            print("\n===== GRID SCRAPING COMPLETE =====")
            print(f"✅ Found {final_results_count} total businesses ({unique_businesses} unique)")
            print(f"Added {new_businesses_found} new businesses this run.")
            print(f"Processed {self.stats['grid_cells_processed']} / {total_cells} grid cells")
            print(f"Duration: {duration:.2f} minutes")
            print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

            return self.results

        except KeyboardInterrupt:
            self.logger.warning("⚠️ Grid scraping interrupted by user")
            print("\n⚠️ Grid scraping interrupted by user. Saving collected data...")
            # Note: Executor might still have running threads. Consider shutdown(wait=False).
            self.save_results()
            return self.results
        except Exception as e:
            self.logger.error(f"❌ Critical error during grid scraping: {e}", exc_info=True)
            print(f"\n❌ Critical error during grid scraping: {e}")
            print("Saving any collected data...")
            self.save_results()
            return self.results
        finally:
            self.browser_pool.close_all()


    def process_grid_cell(self, query, grid_cell):
        """Search, extract links, and process businesses for a single grid cell. Returns the processed cell."""
        cell_id = grid_cell["cell_id"]
        thread_id = threading.get_ident()
        processed_count_in_cell = 0
        max_results_limit = self.config.get("max_results") # Get limit

        self.logger.debug(f"Thread {thread_id} starting processing for cell {cell_id}")

        try:
            # --- Step 1: Search and get links ---
            # search_in_grid_cell handles browser acquisition/release for the search phase
            # It also updates cell processed status and empty status
            business_links = self.search_in_grid_cell(query, grid_cell)

            if not business_links:
                self.logger.debug(f"Thread {thread_id} - No links found in cell {cell_id}. Returning.")
                return grid_cell # Return the cell state updated by search_in_grid_cell

            self.logger.info(f"Thread {thread_id} - Found {len(business_links)} links in {cell_id}. Processing details...")

            # --- Step 2: Process links to get details ---
            # Acquire a browser specifically for processing these links
            detail_browser_id = None
            try:
                detail_browser_id = self.browser_pool.get_browser()
                detail_driver = self.browser_pool.get_driver(detail_browser_id)
                if not detail_driver:
                    raise Exception(f"Failed to get driver for detail extraction in cell {cell_id}")

                for i, link in enumerate(business_links):
                    # Check max results limit BEFORE processing each link
                    with self.lock: current_results_count = len(self.results)
                    if max_results_limit and current_results_count >= max_results_limit:
                        self.logger.info(f"Thread {thread_id} - Max results reached ({max_results_limit}) while processing links in cell {cell_id}. Stopping link processing.")
                        break # Stop processing more links in this cell

                    self.logger.debug(f"Thread {thread_id} - Cell {cell_id}: Processing link {i+1}/{len(business_links)}")
                    place_info = self.extract_place_info(link, detail_driver) # Use the dedicated detail driver

                    if place_info:
                        # Add grid cell info before saving
                        place_info["grid_cell"] = cell_id
                        # Add result to the shared list using the lock
                        with self.lock:
                            business_key = (place_info["name"], place_info.get("address", ""))
                            # Check duplicate again just before adding
                            if business_key not in self.seen_businesses:
                                self.results.append(place_info)
                                self.seen_businesses[business_key] = len(self.results) - 1
                                self.stats["businesses_found"] += 1
                                processed_count_in_cell += 1
                                self.logger.debug(f"Thread {thread_id} - Added place #{len(self.results)}: {place_info['name']} from cell {cell_id}")
                            else:
                                # Handle updates for duplicates if needed (e.g., add email if missing)
                                existing_index = self.seen_businesses[business_key]
                                if place_info.get("email") and not self.results[existing_index].get("email"):
                                     self.results[existing_index]["email"] = place_info["email"]
                                     self.logger.info(f"Thread {thread_id} - Updated email for duplicate: {place_info['name']}")
                                self.logger.debug(f"Thread {thread_id} - Skipping duplicate '{place_info['name']}' found in cell {cell_id}")

                self.logger.info(f"Thread {thread_id} - Finished processing {processed_count_in_cell} new businesses for cell {cell_id}")

            except Exception as detail_err:
                self.logger.error(f"Thread {thread_id} - Error processing links details in cell {cell_id}: {detail_err}", exc_info=True)
                if detail_browser_id is not None: self.browser_pool.report_error(detail_browser_id)
                # Cell is already marked processed by search_in_grid_cell
            finally:
                if detail_browser_id is not None:
                    self.browser_pool.release_browser(detail_browser_id)

            # Save results periodically after processing a cell's links
            if processed_count_in_cell > 0:
                self.save_results() # Save aggregated results

            return grid_cell # Return the cell state

        except Exception as outer_err:
            # Catch errors before detail processing (e.g., error in search_in_grid_cell itself)
            self.logger.error(f"Thread {thread_id} - Major error processing cell {cell_id}: {outer_err}", exc_info=True)
            # Ensure cell is marked processed if error occurred before search_in_grid_cell did it
            with self.lock:
                 if not grid_cell.get("processed"):
                      grid_cell["processed"] = True
                      self.stats["grid_cells_processed"] += 1
                      self.stats["extraction_errors"] += 1
            return grid_cell


    # --- Resume Logic ---
    def load_and_resume(self, results_file, grid_file):
        """Load previous results and grid, and resume scraping from where it left off"""
        # ... (remains largely the same, ensure paths are handled correctly) ...
        try:
            results_path = Path(results_file)
            grid_path = Path(grid_file)

            if results_path.exists():
                with open(results_path, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                for i, result in enumerate(self.results):
                    business_key = (result.get("name", ""), result.get("address", "")) # Handle missing keys
                    if business_key[0]: # Only add if name exists
                         self.seen_businesses[business_key] = i
                    if "maps_url" in result and result["maps_url"]:
                        self.processed_links.add(result["maps_url"])
                self.logger.info(f"Loaded {len(self.results)} businesses from {results_path}")
                print(f"Loaded {len(self.results)} businesses from {results_path}")
            else:
                self.logger.warning(f"Results file {results_path} not found. Starting fresh.")
                print(f"Results file {results_path} not found. Starting fresh.")
                # Allow continuing without results, but grid is needed
                # return False

            if grid_path.exists():
                with open(grid_path, 'r', encoding='utf-8') as f:
                    self.grid = json.load(f)

                # Mark cells as processed based on *loaded* results
                processed_cells_in_results = set()
                for result in self.results:
                    if result.get("grid_cell"):
                        processed_cells_in_results.add(result["grid_cell"])

                processed_count = 0
                empty_count = 0
                for cell in self.grid:
                    # Check if cell ID exists in results OR if it was marked processed previously
                    if cell["cell_id"] in processed_cells_in_results or cell.get("processed"):
                        cell["processed"] = True
                        processed_count += 1
                        if cell.get("likely_empty"):
                             empty_count +=1
                    else:
                         # Ensure unprocessed cells don't have processed flags set
                         cell["processed"] = False
                         cell["likely_empty"] = False


                self.stats["grid_cells_total"] = len(self.grid)
                self.stats["grid_cells_processed"] = processed_count
                self.stats["grid_cells_empty"] = empty_count
                self.stats["businesses_found"] = len(self.results) # Based on loaded results

                self.logger.info(f"Loaded grid with {len(self.grid)} cells from {grid_path}")
                self.logger.info(f"{processed_count} cells marked as processed based on grid file and results.")
                print(f"Loaded grid with {len(self.grid)} cells from {grid_path}")
                print(f"{processed_count} cells marked as processed.")

                self.update_grid_visualization()
                return True
            else:
                self.logger.error(f"Grid file {grid_path} not found. Cannot resume without grid definition.")
                print(f"❌ Grid file {grid_path} not found. Cannot resume.")
                return False

        except Exception as e:
            self.logger.error(f"Error loading previous session: {e}", exc_info=True)
            print(f"❌ Error loading previous session: {e}")
            return False


    def resume_scraping(self, query, max_results=None):
        """Resume scraping from where it left off using parallel workers"""
        if not self.grid:
            self.logger.error("No grid loaded, cannot resume")
            print("No grid loaded, cannot resume")
            return []

        self.stats["start_time"] = datetime.now() # Reset start time for resume duration
        start_time = self.stats["start_time"]
        self.config["max_results"] = max_results # Store limit

        self.logger.info(f"🚀 RESUMING GRID SCRAPING - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.logger.info(f"Query: '{query}'")
        self.logger.info(f"Max results: {max_results or 'unlimited'}")
        self.logger.info(f"Using {self.max_workers} workers")

        print("\n===== RESUMING GRID SCRAPING =====")
        # ... (print statements remain similar) ...
        print(f"Session ID: {self.session_id}")
        print("===================================\n")


        try:
            # Filter unprocessed cells (re-check based on current self.grid state)
            unprocessed_cells = [cell for cell in self.grid if not cell.get("processed", False)]

            if not unprocessed_cells:
                self.logger.info("All cells already processed according to loaded grid. Nothing to do.")
                print("All cells already processed. Nothing to do.")
                return self.results

            total_remaining_cells = len(unprocessed_cells)
            total_grid_cells = self.stats["grid_cells_total"]
            self.logger.info(f"Resuming with {total_remaining_cells} unprocessed cells out of {total_grid_cells}")
            print(f"Resuming with {total_remaining_cells} unprocessed cells...")

            # Sort remaining cells
            unprocessed_cells = self.sort_grid_cells_by_density(unprocessed_cells)

            # Process remaining cells in parallel
            futures = []
            processed_resumed_cells_count = 0
            initial_results_count = len(self.results) # Count before resuming
            stop_submission = False

            with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix='GridResumeWorker') as executor:
                 with tqdm(total=total_remaining_cells, desc="Resuming Grid Cells", unit="cell", smoothing=0.1) as progress_bar:
                    tasks_to_submit = list(unprocessed_cells) # Copy list

                    for cell in tasks_to_submit:
                        with self.lock: current_results_count = len(self.results)
                        if max_results and current_results_count >= max_results:
                            if not stop_submission:
                                 self.logger.info(f"Max results ({max_results}) reached during resume. Stopping submission.")
                                 print(f"\nMax results ({max_results}) reached, waiting for running tasks...")
                                 stop_submission = True
                        if not stop_submission:
                            futures.append(executor.submit(self.process_grid_cell, query, cell))
                        else:
                             progress_bar.update(1) # Update progress for skipped cells


                    for future in concurrent.futures.as_completed(futures):
                        processed_resumed_cells_count += 1
                        try:
                            processed_cell = future.result()
                            if processed_cell:
                                 # Update master grid list
                                 with self.lock:
                                      for idx, c in enumerate(self.grid):
                                           if c['cell_id'] == processed_cell['cell_id']:
                                                self.grid[idx] = processed_cell
                                                break
                        except Exception as exc:
                            self.logger.error(f'A resumed grid cell task generated an exception: {exc}', exc_info=True)

                        progress_bar.update(1)

                        if processed_resumed_cells_count % 20 == 0 or processed_resumed_cells_count == len(futures):
                             self.update_grid_visualization()


            # --- End of parallel processing ---
            self.logger.info("All submitted resume tasks completed.")
            self.save_results()
            self.generate_statistics_report()

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60 # Duration of the resume part
            final_results_count = len(self.results)
            new_businesses_found = final_results_count - initial_results_count
            unique_businesses = len(set((r["name"], r.get("address", "")) for r in self.results))

            self.logger.info(f"✅ RESUMED GRID SCRAPING COMPLETE")
            self.logger.info(f"Found {final_results_count} total businesses ({unique_businesses} unique)")
            self.logger.info(f"Added {new_businesses_found} new businesses this run.")
            self.logger.info(f"Processed {self.stats['grid_cells_processed']} total grid cells ({processed_resumed_cells_count} this run)")
            self.logger.info(f"Resume Duration: {duration:.2f} minutes")

            print("\n===== RESUMED GRID SCRAPING COMPLETE =====")
            print(f"✅ Found {final_results_count} total businesses ({unique_businesses} unique)")
            print(f"Added {new_businesses_found} new businesses this run.")
            print(f"Processed {self.stats['grid_cells_processed']} total grid cells ({processed_resumed_cells_count} this run)")
            print(f"Resume Duration: {duration:.2f} minutes")
            print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")

            return self.results

        except KeyboardInterrupt:
            self.logger.warning("⚠️ Grid scraping interrupted by user during resume")
            print("\n⚠️ Grid scraping interrupted during resume. Saving collected data...")
            self.save_results()
            return self.results
        except Exception as e:
            self.logger.error(f"❌ Error during resumed grid scraping: {e}", exc_info=True)
            print(f"\n❌ Error during resumed grid scraping: {e}")
            print("Saving any collected data...")
            self.save_results()
            return self.results
        finally:
            self.browser_pool.close_all()


    # --- Saving and Cleanup ---
    def save_results(self):
        """Save results to files (CSV, JSON, Excel if possible)"""
        with self.lock: # Ensure exclusive access to self.results while saving
             if not self.results:
                  # self.logger.info("No results to save yet.") # Reduce log noise
                  return
             results_copy = list(self.results) # Save a copy to avoid holding lock too long

        try:
            base_filename = self.results_dir / f"google_maps_data_{self.session_id}"
            standard_filename_base = self.results_dir / "google_maps_data" # Overwrite standard file

            # Save session-specific files
            self.save_to_csv(f"{base_filename}.csv", results_copy)
            self.save_to_json(f"{base_filename}.json", results_copy)
            if PANDAS_AVAILABLE: self.save_to_excel(f"{base_filename}.xlsx", results_copy)

            # Save/Overwrite standard files
            self.save_to_csv(f"{standard_filename_base}.csv", results_copy)
            self.save_to_json(f"{standard_filename_base}.json", results_copy)
            if PANDAS_AVAILABLE: self.save_to_excel(f"{standard_filename_base}.xlsx", results_copy)

            unique_names = len(set(r["name"] for r in results_copy))
            self.logger.info(f"💾 Saved {len(results_copy)} results ({unique_names} unique businesses)")

        except Exception as e:
            self.logger.error(f"Error saving results: {e}", exc_info=True)
            # Try fallback save
            try:
                 fallback_base = Path(f"fallback_gmaps_data_{self.session_id}")
                 self.save_to_csv(f"{fallback_base}.csv", results_copy)
                 self.save_to_json(f"{fallback_base}.json", results_copy)
                 self.logger.info(f"Saved results to fallback location: {fallback_base}.*")
            except Exception as e2:
                 self.logger.error(f"Fallback save failed: {e2}")


    def save_to_csv(self, filename, data):
        """Save results data to CSV file"""
        if not data: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Dynamically determine headers based on all keys present in the data
            all_keys = set()
            social_keys = set()
            for row in data:
                 all_keys.update(row.keys())
                 if isinstance(row.get("social_links"), dict):
                      social_keys.update(f"social_{net}" for net in row["social_links"])

            preferred_order = [
                "name", "category", "address", "location", "coordinates", "phone",
                "email", "website", "maps_url", "rating", "reviews_count",
                "hours", "price_level", "place_id", "grid_cell", "scrape_date"
            ]

            fieldnames = [f for f in preferred_order if f in all_keys]
            remaining_keys = sorted(list((all_keys - set(preferred_order) - {'social_links'}) | social_keys))
            fieldnames.extend(remaining_keys)

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                for result in data:
                     row_data = result.copy()
                     # Flatten social links
                     if isinstance(row_data.get("social_links"), dict):
                          for network, url in row_data["social_links"].items():
                               row_data[f"social_{network}"] = url
                     if "social_links" in row_data: del row_data["social_links"] # Remove original dict
                     writer.writerow(row_data)
            # self.logger.debug(f"CSV saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving CSV to {filename}: {e}", exc_info=True)


    def save_to_json(self, filename, data):
        """Save results data to JSON file"""
        if not data: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, ensure_ascii=False) # Use indent=2 for smaller files
            # self.logger.debug(f"JSON saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving JSON to {filename}: {e}", exc_info=True)


    def save_to_excel(self, filename, data):
        """Save results data to Excel file using Pandas"""
        if not data or not PANDAS_AVAILABLE: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Prepare data for DataFrame, handling social links
            df_data = []
            all_social_networks = set()
            for result in data:
                 row = result.copy()
                 socials = row.pop("social_links", {})
                 if isinstance(socials, dict):
                      for network, url in socials.items():
                           col_name = f"social_{network}"
                           row[col_name] = url
                           all_social_networks.add(col_name)
                 df_data.append(row)

            df = pd.DataFrame(df_data)

            # Define column order
            preferred_order = [
                "name", "category", "address", "location", "coordinates", "phone",
                "email", "website", "maps_url", "rating", "reviews_count",
                "hours", "price_level", "place_id", "grid_cell", "scrape_date"
            ]
            social_cols = sorted(list(all_social_networks))
            final_order = [col for col in preferred_order if col in df.columns]
            other_cols = sorted([col for col in df.columns if col not in preferred_order and col not in social_cols])
            final_order.extend(social_cols)
            final_order.extend(other_cols)

            # Reorder DataFrame columns
            df = df[final_order]

            df.to_excel(filepath, index=False, engine='openpyxl') # Specify engine if needed
            self.logger.info(f"Saved Excel version to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving Excel to {filename}: {e}", exc_info=True)


    def generate_statistics_report(self):
        """Generate a report with statistics about the scraped data"""
        # ... (remains the same) ...
        with self.lock: # Access results safely
             if not self.results:
                  self.logger.warning("No results to generate statistics report")
                  return
             results_copy = list(self.results) # Work with a copy

        try:
            report = defaultdict(int)
            report["total_businesses"] = len(results_copy)
            report["unique_businesses"] = len(set((r.get("name", ""), r.get("address", "")) for r in results_copy if r.get("name")))
            report["categories"] = Counter()
            report["businesses_by_grid_cell"] = Counter()

            ratings = []
            reviews = []

            for result in results_copy:
                if result.get("category"): report["categories"][result["category"]] += 1
                if result.get("email"): report["with_email"] += 1
                if result.get("website"): report["with_website"] += 1
                if result.get("phone"): report["with_phone"] += 1
                if result.get("grid_cell"): report["businesses_by_grid_cell"][result["grid_cell"]] += 1

                if result.get("rating"):
                    try: ratings.append(float(str(result["rating"]).replace(',', '.'))) # Handle comma decimal separator
                    except (ValueError, TypeError): pass
                if result.get("reviews_count"):
                    try: reviews.append(int(str(result["reviews_count"]).replace(',', '').replace(' ', '')))
                    except (ValueError, TypeError): pass

            report["with_rating"] = len(ratings)
            report["avg_rating"] = round(statistics.mean(ratings), 2) if ratings else 0
            report["median_rating"] = round(statistics.median(ratings), 1) if ratings else 0
            report["total_reviews"] = sum(reviews)
            report["avg_reviews"] = round(statistics.mean(reviews), 1) if reviews else 0
            report["median_reviews"] = int(statistics.median(reviews)) if reviews else 0

            # Top categories
            top_categories = {cat: count for cat, count in report["categories"].most_common(15)}
            report["top_categories"] = top_categories

            # Percentages
            total = report["total_businesses"]
            if total > 0:
                 report["email_percentage"] = round((report["with_email"] / total) * 100, 1)
                 report["website_percentage"] = round((report["with_website"] / total) * 100, 1)
                 report["phone_percentage"] = round((report["with_phone"] / total) * 100, 1)
                 report["rating_percentage"] = round((report["with_rating"] / total) * 100, 1)

            if self.stats["start_time"]:
                elapsed_seconds = (datetime.now() - self.stats["start_time"]).total_seconds()
                report["scrape_duration_minutes"] = round(elapsed_seconds / 60, 2)

            # Add scraping stats
            report["scrape_stats"] = {
                 "total_grid_cells": self.stats["grid_cells_total"],
                 "processed_grid_cells": self.stats["grid_cells_processed"],
                 "empty_grid_cells": self.stats["grid_cells_empty"],
                 "consent_pages_handled": self.stats["consent_pages_handled"],
                 "extraction_errors": self.stats["extraction_errors"],
                 "rate_limit_hits": self.stats["rate_limit_hits"],
                 "session_id": self.session_id
            }

            # Save JSON report
            report_filename = self.results_dir / f"statistics_report_{self.session_id}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str) # Use default=str for Counter objects
            self.logger.info(f"Statistics report saved to {report_filename}")

            if MATPLOTLIB_AVAILABLE and not self.no_images:
                self.generate_html_report(report) # Generate HTML if possible

            return report
        except Exception as e:
            self.logger.error(f"Error generating statistics report: {e}", exc_info=True)
            return None


    def generate_html_report(self, stats):
        """Generate HTML report with visualizations"""
        # ... (remains largely the same, ensure paths are correct) ...
        try:
            category_chart_path = None
            info_chart_path = None

            # Create category chart
            if stats.get("top_categories"):
                try:
                    # ... inside generate_html_report method ...
                        fig, ax = plt.subplots(figsize=(12, 7)) # Adjusted size
                        categories = list(stats["top_categories"].keys())
                        counts = list(stats["top_categories"].values())
                        # Create horizontal bar chart for better label readability
                        y_pos = np.arange(len(categories))
                        ax.barh(y_pos, counts, align='center', color='skyblue')
                        ax.set_yticks(y_pos)
                        ax.set_yticklabels(categories)
                        ax.invert_yaxis()  # labels read top-to-bottom
                        ax.set_xlabel('Number of Businesses')
                        ax.set_title('Top 15 Business Categories Found')
                        # Add counts at the end of the bars
                        for i, v in enumerate(counts):
                            ax.text(v + 1, i, str(v), color='blue', va='center', fontweight='bold', fontsize=9)

                        plt.tight_layout()
                        category_chart_path_obj = self.results_dir / f"category_chart_{self.session_id}.png"
                        plt.savefig(category_chart_path_obj)
                        category_chart_path = category_chart_path_obj.name # Use relative name for HTML
                        plt.close(fig)
                        self.logger.info(f"Category chart saved to {category_chart_path_obj}")

                except Exception as chart_err:
                        self.logger.error(f"Failed to generate category chart: {chart_err}")


            # Create information availability chart (pie chart)
            try:
                fig, ax = plt.subplots(figsize=(8, 5))
                info_labels = ['With Email', 'With Website', 'With Phone', 'With Rating']
                info_counts = [
                    stats.get("with_email", 0), stats.get("with_website", 0),
                    stats.get("with_phone", 0), stats.get("with_rating", 0)
                ]
                total_biz = stats.get("total_businesses", 1) # Avoid division by zero
                info_pcts = [(c / total_biz) * 100 for c in info_counts]

                # Use a pie chart for percentages
                labels_pct = [f'{label}\n({pct:.1f}%)' for label, pct in zip(info_labels, info_pcts)]
                ax.pie(info_counts, labels=labels_pct, autopct='%1.1f%%', startangle=90, colors=['#ff9999','#66b3ff','#99ff99','#ffcc99'])
                ax.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
                plt.title('Percentage of Businesses with Key Information')

                plt.tight_layout()
                info_chart_path_obj = self.results_dir / f"info_chart_{self.session_id}.png"
                plt.savefig(info_chart_path_obj)
                info_chart_path = info_chart_path_obj.name # Use relative name
                plt.close(fig)
                self.logger.info(f"Info chart saved to {info_chart_path_obj}")
            except Exception as chart_err:
                self.logger.error(f"Failed to generate info chart: {chart_err}")


            # --- Generate HTML Report ---
            html_content = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Google Maps Scraper Report - {self.session_id}</title>
                <style>
                    body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; line-height: 1.6; color: #333; background-color: #f9f9f9; }}
                    .container {{ max-width: 1200px; margin: 0 auto; background-color: #fff; padding: 20px; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
                    .header {{ background-color: #4285F4; color: white; padding: 20px; text-align: center; margin-bottom: 30px; border-radius: 5px; }}
                    .header h1 {{ margin: 0; font-size: 2em; }} .header p {{ margin: 5px 0 0; font-size: 0.9em; opacity: 0.9; }}
                    .stats-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 20px; margin-bottom: 30px; }}
                    .stat-card {{ background-color: #e8f0fe; border-radius: 5px; padding: 15px; text-align: center; transition: transform 0.2s ease; }}
                    .stat-card:hover {{ transform: translateY(-3px); box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
                    .stat-number {{ font-size: 2.2em; font-weight: bold; color: #1a73e8; margin-bottom: 5px; }}
                    .stat-label {{ font-size: 0.9em; color: #5f6368; }}
                    .section {{ background-color: #fff; border: 1px solid #e0e0e0; border-radius: 5px; padding: 20px; margin-bottom: 30px; }}
                    .section h2 {{ margin-top: 0; color: #4285F4; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; margin-bottom: 20px; }}
                    .chart {{ margin: 20px 0; text-align: center; }} .chart img {{ max-width: 100%; height: auto; border: 1px solid #ddd; border-radius: 4px; }}
                    ul {{ padding-left: 20px; }} li {{ margin-bottom: 8px; }}
                    .footer {{ text-align: center; margin-top: 40px; color: #666; font-size: 12px; border-top: 1px solid #eee; padding-top: 15px; }}
                    table {{ width: 100%; border-collapse: collapse; margin-top: 15px; }}
                    th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; font-size: 0.9em; }}
                    th {{ background-color: #f2f2f2; font-weight: bold; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <div class="header">
                        <h1>Google Maps Scraper Report</h1>
                        <p>Session ID: {self.session_id} | Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    </div>

                    <div class="section">
                        <h2>Overall Summary</h2>
                        <div class="stats-grid">
                            <div class="stat-card"><div class="stat-number">{stats.get("total_businesses", 0)}</div><div class="stat-label">Total Businesses Found</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("unique_businesses", 0)}</div><div class="stat-label">Unique Businesses</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("with_email", 0)}</div><div class="stat-label">With Email ({stats.get("email_percentage", 0):.1f}%)</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("with_website", 0)}</div><div class="stat-label">With Website ({stats.get("website_percentage", 0):.1f}%)</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("with_phone", 0)}</div><div class="stat-label">With Phone ({stats.get("phone_percentage", 0):.1f}%)</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("with_rating", 0)}</div><div class="stat-label">With Rating ({stats.get("rating_percentage", 0):.1f}%)</div></div>
                        </div>
                    </div>

                    <div class="section">
                         <h2>Ratings & Reviews</h2>
                         <div class="stats-grid">
                            <div class="stat-card"><div class="stat-number">{stats.get("avg_rating", 0):.2f}</div><div class="stat-label">Average Rating</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("median_rating", 0):.1f}</div><div class="stat-label">Median Rating</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("total_reviews", 0):,}</div><div class="stat-label">Total Reviews</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("avg_reviews", 0):.1f}</div><div class="stat-label">Average Reviews</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("median_reviews", 0):,}</div><div class="stat-label">Median Reviews</div></div>
                        </div>
                    </div>

                    <div class="section">
                        <h2>Business Categories</h2>
                        {'<div class="chart"><img src="' + category_chart_path + '" alt="Business Categories Chart"></div>' if category_chart_path else "<p>Category chart could not be generated.</p>"}
                        {'<table><thead><tr><th>Category</th><th>Count</th></tr></thead><tbody>' + ''.join([f'<tr><td>{cat}</td><td>{count}</td></tr>' for cat, count in stats.get("top_categories", {}).items()]) + '</tbody></table>' if stats.get("top_categories") else ""}
                    </div>

                    <div class="section">
                        <h2>Information Availability</h2>
                        {'<div class="chart"><img src="' + info_chart_path + '" alt="Information Availability Chart"></div>' if info_chart_path else "<p>Info availability chart could not be generated.</p>"}
                    </div>

                    <div class="section">
                        <h2>Scraping Performance</h2>
                        <ul>
                            <li>Scrape Duration: {stats.get("scrape_duration_minutes", 0):.2f} minutes</li>
                            <li>Total Grid Cells: {stats.get("scrape_stats", {}).get("total_grid_cells", 0)}</li>
                            <li>Processed Grid Cells: {stats.get("scrape_stats", {}).get("processed_grid_cells", 0)}</li>
                            <li>Empty Grid Cells Found: {stats.get("scrape_stats", {}).get("empty_grid_cells", 0)}</li>
                            <li>Consent Pages Handled: {stats.get("scrape_stats", {}).get("consent_pages_handled", 0)}</li>
                            <li>Extraction Errors / Skips: {stats.get("scrape_stats", {}).get("extraction_errors", 0)}</li>
                            <li>Potential Rate Limit Hits: {stats.get("scrape_stats", {}).get("rate_limit_hits", 0)}</li>
                            <li>Max Workers Used: {self.max_workers}</li>
                        </ul>
                    </div>

                    <div class="footer">
                        <p>Generated by Google Maps Grid Scraper v{VERSION}</p>
                    </div>
                </div>
            </body>
            </html>
            """

            # Save HTML report
            html_report_path = self.results_dir / f"report_{self.session_id}.html"
            with open(html_report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"HTML report saved to {html_report_path}")

        except Exception as e:
            self.logger.error(f"Error generating HTML report: {e}", exc_info=True)


    def sort_grid_cells_by_density(self, grid):
        """Sort grid cells by likely density of businesses (center of city first)"""
        if len(grid) <= 4: return grid # Skip sorting for very small grids

        # Find grid bounds
        min_row = min(cell["row"] for cell in grid)
        max_row = max(cell["row"] for cell in grid)
        min_col = min(cell["col"] for cell in grid)
        max_col = max(cell["col"] for cell in grid)

        # Find center indices relative to the actual grid cells present
        center_row = (min_row + max_row) / 2
        center_col = (min_col + max_col) / 2

        # Calculate distance from center for each cell
        for cell in grid:
            row_distance = abs(cell["row"] - center_row)
            col_distance = abs(cell["col"] - center_col)
            # Use Euclidean distance for a more circular priority, Manhattan for diamond
            # cell["center_distance"] = row_distance + col_distance # Manhattan
            cell["center_distance"] = math.sqrt(row_distance**2 + col_distance**2) # Euclidean

        # Sort by distance (ascending)
        sorted_grid = sorted(grid, key=lambda x: x["center_distance"])
        self.logger.info("Sorted grid cells by distance from center.")
        return sorted_grid


    def close(self):
        """Close browsers and cleanup resources"""
        self.logger.info("Initiating shutdown sequence...")
        try:
            # Final save attempt
            self.logger.info("Performing final save...")
            self.save_results()
            with self.lock:
                 final_count = len(self.results)
            self.logger.info(f"Final save completed with {final_count} businesses.")
        except Exception as e:
            self.logger.error(f"Error during final save: {e}", exc_info=True)

        # Close browser pool
        if hasattr(self, 'browser_pool'):
            self.browser_pool.close_all()

        self.logger.info("Scraper resources cleaned up.")
        logging.shutdown() # Flush and close all logging handlers


# --- Main Execution Functions ---
def run_grid_scraper():
    """Run the enhanced Google Maps Grid Scraper with CLI arguments"""
    parser = argparse.ArgumentParser(description=f'Enhanced Google Maps Grid Scraper v{VERSION}')

    # Basic arguments
    parser.add_argument('-q', '--query', type=str, help='Search query (e.g., "restaurants")')
    parser.add_argument('-l', '--location', type=str, help='Location to search in (e.g., "New York")')
    parser.add_argument('--headless', action=argparse.BooleanOptionalAction, default=True, help='Run in headless mode (default) or visible (--no-headless)')
    parser.add_argument('--grid-size', type=int, default=250, help='Grid cell size in meters (default: 250)')
    parser.add_argument('--max-results', type=int, help='Maximum number of results to collect')
    parser.add_argument('--workers', type=int, default=5, help='Number of parallel browser workers (default: 5)')

    # Advanced arguments
    parser.add_argument('--debug', action='store_true', help='Enable debug mode (more logging, some screenshots)')
    parser.add_argument('--no-cache', dest='cache_enabled', action='store_false', default=True, help='Disable caching of bounds')
    parser.add_argument('--no-emails', dest='extract_emails', action='store_false', default=True, help='Skip email extraction from websites')
    parser.add_argument('--no-images', action='store_true', help='Disable saving ALL screenshots and visualization images')
    parser.add_argument('--retries', type=int, default=3, help='Browser health error threshold before recreating (default: 3)')
    parser.add_argument('--proxies', type=str, help='File containing list of proxies (one per line, e.g., http://user:pass@host:port)')

    # Resume arguments
    parser.add_argument('--resume', action='store_true', help='Resume from previous session (requires --results-file and --grid-file)')
    parser.add_argument('--results-file', type=str, help='Results JSON file to load for resume')
    parser.add_argument('--grid-file', type=str, help='Grid definition JSON file to load for resume')

    args = parser.parse_args()

    # If no arguments provided, switch to interactive mode
    # Check if only the script name itself is present (sys.argv[0])
    # Or if specific args required for non-resume are missing
    if len(sys.argv) <= 1 or (not args.resume and (not args.query or not args.location)):
        if len(sys.argv) > 1 and (not args.query or not args.location) and not args.resume :
             print("\nError: --query and --location are required for a new scrape.")
             parser.print_help()
             sys.exit(1)
        else:
             # Run interactive mode if just 'python script.py' is called
             return run_interactive_grid_scraper()

    # Validate resume arguments
    if args.resume and (not args.results_file or not args.grid_file):
        print("\nError: --results-file and --grid-file are required when using --resume")
        parser.print_help()
        sys.exit(1)

    # Load proxies if specified
    proxy_list = []
    if args.proxies:
        try:
            proxy_path = Path(args.proxies)
            if proxy_path.is_file():
                with open(proxy_path, 'r') as f:
                    proxy_list = [line.strip() for line in f if line.strip()]
                print(f"Loaded {len(proxy_list)} proxies from {args.proxies}")
            else:
                print(f"Warning: Proxy file '{args.proxies}' not found.")
        except Exception as e:
            print(f"Error loading proxies: {e}")


    # Create and run the scraper
    scraper = None # Initialize scraper to None for finally block
    try:
        print("\n🌍 ENHANCED GOOGLE MAPS GRID SCRAPER 🌍")
        print("=========================================")
        print(f"🔄 Initializing with {args.workers} workers...\n")

        scraper = GoogleMapsGridScraper(
            headless=args.headless,
            max_workers=args.workers,
            debug=args.debug,
            cache_enabled=args.cache_enabled,
            retry_attempts=args.retries, # This is browser health threshold now
            no_images=args.no_images,
            proxy_list=proxy_list
        )

        # Set configuration options
        scraper.config["extract_emails"] = args.extract_emails
        scraper.config["grid_size_meters"] = args.grid_size # Store grid size

        # Run the scraper
        if args.resume:
            print(f"Attempting to resume scraping from:")
            print(f" - Results: {args.results_file}")
            print(f" - Grid:    {args.grid_file}")
            # Query is needed even for resume to know what to search in new cells
            if not args.query:
                 print("\nError: --query is required even when resuming (to search in remaining cells).")
                 sys.exit(1)

            if scraper.load_and_resume(args.results_file, args.grid_file):
                results = scraper.resume_scraping(args.query, args.max_results)
            else:
                print("\n❌ Failed to load previous session data properly. Cannot resume.")
                return # Exit if resume failed
        else:
            results = scraper.scrape(args.query, args.location, args.grid_size, args.max_results)

        # --- Final Output ---
        if results is not None: # Check if scraping ran without critical error
            print(f"\n✅ Scraping finished. Found {len(results)} total businesses in results file(s).")
            # Calculate unique based on saved results
            unique_businesses = len(set((r.get("name", ""), r.get("address", "")) for r in results if r.get("name")))
            emails_found = sum(1 for r in results if r.get("email"))
            print(f"   - Unique Businesses: {unique_businesses}")
            print(f"   - Businesses with Email: {emails_found}")

            results_dir = Path("results")
            session_csv_path = results_dir / f"Maps_data_{scraper.session_id}.csv"
            standard_csv_path = results_dir / "Maps_data.csv"
            print(f"\nData saved to '{results_dir}' directory:")
            print(f"  - Session CSV: {session_csv_path.name}")
            print(f"  - Latest CSV:  {standard_csv_path.name}")
            print("  - (JSON and potentially XLSX versions also saved)")
            print(f"  - Statistics Report (JSON): statistics_report_{scraper.session_id}.json")
            print(f"  - HTML Report: report_{scraper.session_id}.html")

        else:
            print("\n⚠️ Scraping process did not complete successfully or found no results.")

        print(f"\n📄 Logs saved in 'logs' folder (Session ID: {scraper.session_id})")

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        if scraper:
            print("Attempting to save results before exit...")
            scraper.save_results()
            print("Results saved.")

    except Exception as e:
        print(f"\n❌ An unexpected critical error occurred: {e}")
        print("Check logs for detailed traceback.")
        logging.getLogger("GoogleMapsScraper").error("Critical error in main execution", exc_info=True) # Log exception before closing
        if scraper:
            print("Attempting to save any collected results...")
            scraper.save_results()

    finally:
        # Always attempt to close the scraper and browser pool
        if scraper:
            scraper.close()
        print("\n👋 Scraper closed. Thank you!")


def run_interactive_grid_scraper():
    """Run the grid scraper in interactive mode with prompts"""
    print("\n🌍 ENHANCED GOOGLE MAPS GRID SCRAPER (Interactive Mode) 🌍")
    print("============================================================")

    # Resume option first
    resume = input("\nResume from a previous session? (y/n, default: n): ").strip().lower() == 'y'
    results_file = None
    grid_file = None
    query = ""
    location = ""
    grid_size = 250
    max_results = None

    if resume:
        print("\n--- Resume Setup ---")
        results_input = input("Enter path to results JSON file (e.g., results/Maps_data.json): ").strip()
        if not results_input or not Path(results_input).exists():
            print(f"Results file '{results_input}' not found. Cannot resume.")
            return
        results_file = results_input

        grid_input = input("Enter path to grid definition JSON file (e.g., grid_data/grid_definition_...json): ").strip()
        if not grid_input or not Path(grid_input).exists():
             print(f"Grid file '{grid_input}' not found. Cannot resume.")
             return
        grid_file = grid_input

        while not query:
             query = input("What type of business were you searching for? (Required for resume): ").strip()

    else:
        print("\n--- New Scrape Setup ---")
        while not query:
             query = input("What type of business to search for? (e.g., hotels, plumbers): ").strip()
        while not location:
             location = input("Location? (e.g., Prague CZ, London UK, 10001): ").strip()

        try:
            grid_size_input = input("Grid size in meters (200-1000, default: 250): ")
            grid_size = int(grid_size_input) if grid_size_input else 250
            grid_size = max(100, min(2000, grid_size)) # Wider range, min 100m
        except ValueError:
            grid_size = 250
        print(f"   Using grid size: {grid_size} meters")

    # Options applicable to both modes
    print("\n--- Configuration Options ---")
    try:
        max_results_input = input("Maximum results to collect? (leave empty for unlimited): ").strip()
        max_results = int(max_results_input) if max_results_input else None
    except ValueError:
        max_results = None
    print(f"   Max results: {'Unlimited' if max_results is None else max_results}")

    headless_mode = input("Run headless (hidden browser)? (y/n, default: y): ").strip().lower() != 'n'
    print(f"   Headless mode: {'Yes' if headless_mode else 'No'}")

    debug_mode = input("Enable debug mode (more logs/screenshots)? (y/n, default: n): ").strip().lower() == 'y'
    print(f"   Debug mode: {'Yes' if debug_mode else 'No'}")

    extract_emails = input("Extract emails from websites (slower)? (y/n, default: y): ").strip().lower() != 'n'
    print(f"   Extract emails: {'Yes' if extract_emails else 'No'}")

    try:
        workers_input = input(f"Number of parallel workers? (1-{os.cpu_count()*5 or 10}, default: 5): ").strip() # Default 5, suggest based on CPU
        max_workers = int(workers_input) if workers_input else 5
        max_workers = max(1, min(os.cpu_count()*10 or 20, max_workers)) # Limit workers reasonably
    except ValueError:
        max_workers = 5
    print(f"   Parallel workers: {max_workers}")

    # --- Initialize and Run ---
    scraper = None
    try:
        print("\nInitializing scraper...")
        scraper = GoogleMapsGridScraper(
            headless=headless_mode,
            max_workers=max_workers,
            debug=debug_mode,
            no_images=debug_mode is False # Disable images if not debugging by default
        )
        scraper.config["extract_emails"] = extract_emails
        scraper.config["grid_size_meters"] = grid_size

        print("\nStarting scraper...")
        if resume:
            if scraper.load_and_resume(results_file, grid_file):
                results = scraper.resume_scraping(query, max_results)
            else:
                print("\n❌ Failed to load session data. Cannot resume.")
                results = None # Indicate failure
        else:
            results = scraper.scrape(query, location, grid_size, max_results)

        # --- Final Output --- (Same as CLI version)
        if results is not None:
            print(f"\n✅ Scraping finished. Found {len(results)} total businesses in results file(s).")
            unique_businesses = len(set((r.get("name", ""), r.get("address", "")) for r in results if r.get("name")))
            emails_found = sum(1 for r in results if r.get("email"))
            print(f"   - Unique Businesses: {unique_businesses}")
            print(f"   - Businesses with Email: {emails_found}")

            results_dir = Path("results")
            session_csv_path = results_dir / f"Maps_data_{scraper.session_id}.csv"
            standard_csv_path = results_dir / "Maps_data.csv"
            print(f"\nData saved to '{results_dir}' directory:")
            print(f"  - Session CSV: {session_csv_path.name}")
            print(f"  - Latest CSV:  {standard_csv_path.name}")
            print("  - (JSON and potentially XLSX versions also saved)")
            print(f"  - Statistics Report (JSON): statistics_report_{scraper.session_id}.json")
            print(f"  - HTML Report: report_{scraper.session_id}.html")

        else:
            print("\n⚠️ Scraping process did not complete successfully or found no results.")

        print(f"\n📄 Logs saved in 'logs' folder (Session ID: {scraper.session_id})")


    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        if scraper:
            print("Attempting to save results before exit...")
            scraper.save_results()
            print("Results saved.")
    except Exception as e:
        print(f"\n❌ An unexpected critical error occurred in interactive mode: {e}")
        print("Check logs for detailed traceback.")
        logging.getLogger("GoogleMapsScraper").error("Critical error in interactive execution", exc_info=True)
        if scraper:
            print("Attempting to save any collected results...")
            scraper.save_results()
    finally:
        if scraper:
            scraper.close()
        print("\n👋 Interactive session ended. Thank you!")


# --- Entry Point ---
if __name__ == "__main__":
    # Critical: Ensure directories exist right at the start
    ensure_directories_exist()
    # Let run_grid_scraper handle whether to run CLI or interactive mode
    run_grid_scraper()
