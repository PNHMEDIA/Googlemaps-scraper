#!/usr/bin/env python3

# Standard Library Imports
import argparse
import calendar
import concurrent.futures
import csv
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import quote, urlparse, parse_qs
import statistics

# Selenium Imports
from selenium import webdriver
from selenium.common.exceptions import (NoSuchElementException,
                                        StaleElementReferenceException,
                                        TimeoutException, WebDriverException)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.action_chains import ActionChains

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
    colorama.init(autoreset=True) # Enable autoreset
    COLORAMA_AVAILABLE = True
except ImportError:
    COLORAMA_AVAILABLE = False
    print("Colorama not available. Colored output disabled.")

# --- Global Constants ---
VERSION = "4.0.0" # Updated version
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.5 Mobile/15E148 Safari/604.1"
]

# Updated selectors for 2024 Google Maps
MAPS_URL_PATTERN = re.compile(r"https://(?:www\.)?google\.[a-z.]+/maps/place/.+/@-?\d+\.\d+,-?\d+\.\d+")

# Updated selectors - 2024 Google Maps structure
RESULTS_PANEL_SELECTOR = "div[role='feed'], div[role='list'], div.m6QErb[role='region']"
RESULT_ITEM_SELECTOR = "a[href*='/maps/place/'], div[jsaction*='mouseover']:has(a[href*='/maps/place/']), div.Nv2PK"
RESULT_LINK_SELECTOR = "a[href*='/maps/place/']"
END_OF_RESULTS_XPATH = "//*[contains(text(), \"You've reached the end of the list\") or contains(text(), \"End of list\") or contains(text(), \"No results found\") or contains(text(), \"Aucun résultat\") or contains(text(), \"Keine Ergebnisse\") or contains(text(), \"Nessun risultato\") or contains(text(), \"No se han encontrado resultados\")]"

# JS Injection scripts to bypass detection
STEALTH_JS = """
// Override the webdriver property
Object.defineProperty(navigator, 'webdriver', {
  get: () => false,
});

// Override the permissions property
if (navigator.permissions) {
  const originalQuery = navigator.permissions.query;
  navigator.permissions.query = (parameters) => (
    parameters.name === 'notifications' ?
      Promise.resolve({ state: Notification.permission }) :
      originalQuery(parameters)
  );
}

// Override plugins
Object.defineProperty(navigator, 'plugins', {
  get: () => {
    const plugins = [];
    for (let i = 0; i < 3; i++) {
      plugins.push({
        name: `Plugin ${i + 1}`,
        description: `Description ${i + 1}`,
        filename: `plugin_${i + 1}.dll`,
        length: 3,
        item: function(index) { return this[index] || null; },
        namedItem: function(name) { return null; },
        0: { type: 'application/x-shockwave-flash', suffixes: 'swf', description: 'Shockwave Flash' },
        1: { type: 'application/pdf', suffixes: 'pdf', description: 'PDF Viewer' },
        2: { type: 'application/x-test', suffixes: 'test', description: 'Test Plugin' }
      });
    }
    return plugins;
  }
});

// Add language and platform
Object.defineProperty(navigator, 'language', {
  get: () => 'en-US',
});

Object.defineProperty(navigator, 'platform', {
  get: () => 'Win32',
});

// Add canvas fingerprint noise
HTMLCanvasElement.prototype.getContext = (function(origFn) {
  return function(type, attributes) {
    const context = origFn.call(this, type, attributes);
    if (type === '2d') {
      const oldGetImageData = context.getImageData;
      context.getImageData = function(sx, sy, sw, sh) {
        const imageData = oldGetImageData.call(this, sx, sy, sw, sh);
        // Add some noise
        for (let i = 0; i < imageData.data.length; i += 4) {
          const noise = Math.floor(Math.random() * 3) - 1;
          imageData.data[i] = Math.max(0, Math.min(255, imageData.data[i] + noise));
          imageData.data[i+1] = Math.max(0, Math.min(255, imageData.data[i+1] + noise));
          imageData.data[i+2] = Math.max(0, Math.min(255, imageData.data[i+2] + noise));
        }
        return imageData;
      };
    }
    return context;
  };
})(HTMLCanvasElement.prototype.getContext);

// Add Chrome-specific functions to simulate Chrome
window.chrome = {
  runtime: {},
  loadTimes: function() {},
  csi: function() {},
  app: {}
};
"""

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
        super().__init__(fmt="%(asctime)s - %(levelname)s - Thread %(threadName)s - %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
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
        # Apply color only to the log level for better readability
        log_level_colored = f"{color}{levelname}{self.reset}"
        # Reconstruct message with colored level
        # Find the position of the original level name to replace it
        level_start_index = formatted_message.find(levelname)
        if level_start_index != -1:
            formatted_message = formatted_message[:level_start_index] + log_level_colored + formatted_message[level_start_index + len(levelname):]
        else: # Fallback if level name not found (shouldn't happen with standard format)
             formatted_message = f"{color}{formatted_message}{self.reset}"

        return formatted_message


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
    logger.setLevel(logging.DEBUG) # Capture all levels
    if logger.hasHandlers(): logger.handlers.clear()

    # Console handler (INFO level, colored)
    console_handler = logging.StreamHandler(sys.stdout) # Use stdout for console
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(ColorFormatter())

    # Main file handler (DEBUG level, includes thread name)
    main_log_file = log_dir / f"gmaps_scraper_{session_id}.log"
    main_file_handler = logging.FileHandler(main_log_file, encoding='utf-8')
    main_file_handler.setLevel(logging.DEBUG)
    main_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s'))

    # Error file handler (WARNING level and above, includes file/line)
    error_log_file = log_dir / f"gmaps_errors_{session_id}.log"
    error_file_handler = logging.FileHandler(error_log_file, encoding='utf-8')
    error_file_handler.setLevel(logging.WARNING)
    error_file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(pathname)s:%(lineno)d\n%(message)s\n'))

    logger.addHandler(console_handler)
    logger.addHandler(main_file_handler)
    logger.addHandler(error_file_handler)

    # --- Specific Loggers (Optional but good practice) ---
    # Grid debug logger
    grid_logger = logging.getLogger("GridDebug")
    grid_logger.setLevel(logging.DEBUG)
    if grid_logger.hasHandlers(): grid_logger.handlers.clear()
    grid_log_file = log_dir / f"grid_debug_{session_id}.log"
    grid_handler = logging.FileHandler(grid_log_file, encoding='utf-8')
    grid_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))
    grid_logger.addHandler(grid_handler)
    grid_logger.propagate = False # Prevent grid logs from going to main logger console

    # Business data logger (for raw data output)
    business_logger = logging.getLogger("BusinessData")
    business_logger.setLevel(logging.INFO)
    if business_logger.hasHandlers(): business_logger.handlers.clear()
    business_log_file = log_dir / f"business_data_{session_id}.log"
    business_handler = logging.FileHandler(business_log_file, encoding='utf-8')
    business_handler.setFormatter(logging.Formatter('%(message)s')) # Keep simple for data logging
    business_logger.addHandler(business_handler)
    business_logger.propagate = False

    return logger, grid_logger, business_logger

# --- Core Classes ---
class BrowserPool:
    """Manages a pool of browser instances for parallel processing"""
    def __init__(self, max_browsers=5, headless=True, proxy_list=None, debug=False, browser_error_threshold=3, 
                 user_data_dir=None, driver_path=None, chrome_binary=None):
        self.max_browsers = max_browsers
        self.headless = headless
        self.proxy_list = proxy_list or []
        self.debug = debug
        self.browser_error_threshold = browser_error_threshold
        self.lock = threading.Lock()
        self.browsers = {} # Use dict: id -> browser instance
        self.browser_in_use = {} # id -> bool
        self.browser_health = {} # id -> dict (errors, pages_loaded, creation_time)
        self.next_browser_id = 0
        self.logger = logging.getLogger("GoogleMapsScraper")
        self.user_data_dir = user_data_dir
        self.driver_path = driver_path
        self.chrome_binary = chrome_binary

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
                        self.browser_health[new_id] = {"errors": 0, "pages_loaded": 0, "creation_time": time.time()}
                        self.next_browser_id += 1
                        self.logger.info(f"Thread {thread_id} created and acquired new browser #{new_id} (Pool size: {len(self.browsers)}/{self.max_browsers})")
                        return new_id
                    except Exception as e:
                        self.logger.error(f"Thread {thread_id} failed to create browser: {e}", exc_info=self.debug)
                        # Don't immediately retry creation in case of systemic issue
                        time.sleep(2) # Wait before next attempt cycle

            # If no browser acquired or created, wait before checking again
            self.logger.debug(f"Thread {thread_id} waiting for browser...")
            time.sleep(random.uniform(0.5, 1.5)) # Random sleep to avoid thundering herd

        self.logger.error(f"Thread {thread_id} timed out waiting for browser after {timeout}s")
        raise TimeoutError(f"No browser available in the pool within {timeout} seconds")

    def _create_browser(self):
        """Create a new browser instance with improved anti-detection measures"""
        options = Options()
        
        # Configure headless mode properly
        if self.headless:
            options.add_argument("--headless=new") # Modern headless mode
        
        # Set standard window size
        options.add_argument("--window-size=1920,1080")
        
        # Security and performance settings
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        
        # Anti-detection settings
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Language setting (en-US is most common)
        options.add_argument("--lang=en-US")
        
        # Additional browser configurations
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-popup-blocking")
        
        # Randomize user agent
        random_user_agent = random.choice(USER_AGENTS)
        options.add_argument(f"user-agent={random_user_agent}")
        
        # Add more realistic browser fingerprint
        # Set standard timezone for consistency
        options.add_argument("--timezone=America/New_York")  # Common timezone
        
        # Add geolocation permissions
        prefs = {
            "profile.default_content_setting_values.geolocation": 1,  # 1 = Allow
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_settings.popups": 0
        }
        options.add_experimental_option("prefs", prefs)
        
        # Use custom user data directory if provided
        if self.user_data_dir:
            unique_profile = os.path.join(self.user_data_dir, f"profile_{hash_string(str(time.time()))}")
            options.add_argument(f"--user-data-dir={unique_profile}")
        
        # Add proxy if available
        if self.proxy_list:
            proxy = random.choice(self.proxy_list)
            options.add_argument(f'--proxy-server={proxy}')
            self.logger.debug(f"Using proxy: {proxy}")
            
        # Create service object if driver path is provided
        service = None
        if self.driver_path:
            service = Service(executable_path=self.driver_path)
        
        # Set chrome binary path if provided
        if self.chrome_binary:
            options.binary_location = self.chrome_binary

        # Create the browser
        try:
            if service:
                browser = webdriver.Chrome(service=service, options=options)
            else:
                browser = webdriver.Chrome(options=options)
                
            # Set page load and script timeouts
            browser.set_page_load_timeout(60)
            browser.set_script_timeout(60)
            
            # Inject stealth JS to avoid detection
            browser.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": STEALTH_JS
            })
            
            self.logger.debug("Browser instance created successfully with anti-detection measures")
            return browser
            
        except WebDriverException as e:
            self.logger.error(f"WebDriverException during browser creation: {e}")
            if "net::ERR_PROXY_CONNECTION_FAILED" in str(e) and self.proxy_list:
                 self.logger.error("Proxy connection failed. Check proxy settings/availability.")
            elif "unable to connect to renderer" in str(e).lower():
                 self.logger.error("Browser renderer connection issue. Try updating Chrome/ChromeDriver or disabling headless.")
            raise

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

    def report_error(self, browser_id, error_type="Generic"):
        """Report an error with a browser, potentially recreating it"""
        thread_id = threading.get_ident()
        with self.lock:
            if browser_id not in self.browser_health:
                self.logger.warning(f"Thread {thread_id} reported error for non-existent browser #{browser_id}")
                return # Cannot report error for a browser that doesn't exist in the pool

            self.browser_health[browser_id]["errors"] += 1
            error_count = self.browser_health[browser_id]["errors"]
            self.logger.warning(f"Thread {thread_id} reported {error_type} error for browser #{browser_id} (Error count: {error_count}/{self.browser_error_threshold})")

            # If too many errors, recreate the browser
            if error_count >= self.browser_error_threshold:
                self.logger.warning(f"Browser #{browser_id} reached error threshold ({error_count}), recreating...")
                try:
                    old_browser = self.browsers.get(browser_id)
                    if old_browser:
                        try:
                            old_browser.quit()
                            self.logger.debug(f"Old browser #{browser_id} quit successfully.")
                        except Exception as quit_err:
                            self.logger.warning(f"Error quitting old browser #{browser_id}: {quit_err}")
                        # Remove from dict regardless of quit success
                        del self.browsers[browser_id]

                    # Create replacement
                    new_browser = self._create_browser()
                    self.browsers[browser_id] = new_browser # Replace in dict with same ID
                    self.browser_health[browser_id] = {"errors": 0, "pages_loaded": 0, "creation_time": time.time()} # Reset health
                    # Keep browser marked as in_use as the calling thread still holds it
                    self.logger.info(f"Thread {thread_id} successfully recreated browser #{browser_id}")

                except Exception as e:
                    self.logger.error(f"Thread {thread_id} failed during recreation of browser #{browser_id}: {e}", exc_info=self.debug)
                    # If recreation fails, remove the problematic browser ID entirely
                    if browser_id in self.browsers: del self.browsers[browser_id]
                    if browser_id in self.browser_in_use: del self.browser_in_use[browser_id]
                    if browser_id in self.browser_health: del self.browser_health[browser_id]
                    self.logger.error(f"Removed problematic browser ID {browser_id} from pool after recreation failure.")

    def get_driver(self, browser_id):
        """Get the actual driver instance for a browser_id"""
        # No lock needed for read if assignment is atomic, but safer with lock for consistency
        with self.lock:
            return self.browsers.get(browser_id) # Use .get for safety

    def close_all(self):
        """Close all browsers in the pool"""
        self.logger.info(f"Closing all {len(self.browsers)} browsers in the pool...")
        with self.lock:
            browser_ids_to_close = list(self.browsers.keys()) # Get IDs first
            for browser_id in browser_ids_to_close:
                browser = self.browsers.pop(browser_id, None) # Remove from dict safely
                if browser:
                    try:
                        browser.quit()
                        self.logger.debug(f"Closed browser #{browser_id}")
                    except Exception as e:
                        self.logger.warning(f"Error closing browser #{browser_id}: {e}")
                # Clean up other tracking dicts
                if browser_id in self.browser_in_use: del self.browser_in_use[browser_id]
                if browser_id in self.browser_health: del self.browser_health[browser_id]

            # Clear all tracking dicts just in case
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
        if not self.enabled or not self.cache_dir.exists(): return
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
        temp_path = cache_path.with_suffix(".tmp")
        try:
            with self.lock:
                # Write to a temporary file first
                with open(temp_path, 'w', encoding='utf-8') as f:
                    json.dump(value, f, ensure_ascii=False, indent=2) # Add indent for readability
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
            {"url_pattern": "accounts.google.com/signin", "severity": "high"}, # Login page
            {"url_pattern": "accounts.google.com", "severity": "medium"}, # Other account pages
            {"url_pattern": "/maps/preview/consent", "severity": "medium"},
            {"url_pattern": "_/consentview", "severity": "medium"},
            {"url_pattern": "consent_flow", "severity": "medium"}
        ]
        # Updated 2024 button texts/selectors
        self.accept_selectors = [
            # Common Google consent buttons in different languages
            "//button[.//span[contains(text(), 'Accept all')]]",
            "//button[.//span[contains(text(), 'Accetta tutto')]]",
            "//button[.//span[contains(text(), 'Tout accepter')]]",
            "//button[.//span[contains(text(), 'Alle akzeptieren')]]",
            "//button[.//span[contains(text(), 'Aceptar todo')]]",
            "//button[.//span[contains(text(), 'Aceitar tudo')]]",
            "//button[.//span[contains(text(), 'Alles accepteren')]]",
            "//button[.//span[contains(text(), 'Acceptér alle')]]",
            "//button[.//span[contains(text(), 'I agree')]]",
            "//button[.//span[contains(text(), 'Sono d')]]", # d'accordo
            "//button[.//span[contains(text(), 'J\'accepte')]]",
            "//button[.//span[contains(text(), 'Ich stimme zu')]]",
            "//button[.//span[contains(text(), 'Estoy de acuerdo')]]",
            "//button[.//span[contains(text(), 'Concordo')]]",
            "//button[.//span[contains(text(), 'Ik ga akkoord')]]",
            # Direct button text matches (fallback)
            "//button[normalize-space()='Accept all']",
            "//button[normalize-space()='I agree']",
            "//button[normalize-space()='Agree']",
            "//button[normalize-space()='Continue']",
            # Specific IDs/Classes (updated for 2024)
            "button#L2AGLb", # Common Google consent ID
            "button[jsname='j6LnEc']", # Consent button jsname
            "button[jsname='higCR']", # Another consent button jsname
            "button.VfPpkd-LgbsSe-OWXEXe-k8QpJ", # Material design button class
            "form[action*='signin'] button", # Button within a sign-in form
            "button[data-testid='accept-button']",
            # Role-based selectors
            "//button[@role='button' and contains(., 'Accept')]",
            "//button[@role='button' and contains(., 'Agree')]",
            "//div[@role='dialog']//button[contains(., 'Accept')]", # Button within a dialog
        ]
        self.cookie_banner_selectors = [
             "#onetrust-accept-btn-handler",      # OneTrust banner
             ".cc-banner .cc-btn",                # Cookieconsent banner
             ".cookie-notice button",
             ".cookie-banner button",
             ".consent-banner button",
             "#cookie-popup button",
             ".gdpr button",
             "div[aria-label*='cookie'] button[aria-label*='Accept']", # More generic cookie banner
             "div[id*='cookie'] button[id*='accept']",
             "div[class*='cookie'] button[class*='accept']",
             "#CybotCookiebotDialogBodyButtonAccept", # CookieBot
             "#accept-cookies", # Generic ID
             "button[data-cookiebanner='accept_all']", # GDPR cookie banner
        ]

    def handle_consent(self, driver, take_screenshot=False, debug_dir=None):
        """Handle various Google consent pages and popups. Returns True if handled, False otherwise."""
        handled = False
        try:
            current_url = driver.current_url
            page_title = driver.title.lower()
            consent_detected = any(p["url_pattern"] in current_url for p in self.consent_patterns) or "consent" in page_title or "sign in" in page_title

            if consent_detected:
                severity = next((p["severity"] for p in self.consent_patterns if p["url_pattern"] in current_url), "low")
                self.logger.info(f"⚠️ Detected potential consent/login page ({severity}): {current_url}")

                if take_screenshot and debug_dir:
                    self._take_consent_screenshot(driver, debug_dir, "consent_page")

                # Try multiple strategies to handle consent pages
                if self._try_click_elements(driver, self.accept_selectors, "consent accept button"):
                    self.logger.info("Consent handled by clicking common accept button.")
                    time.sleep(random.uniform(1.5, 2.5)) # Wait for page redirect/update
                    handled = True
                else:
                    # Try executing JavaScript to bypass consent
                    try:
                        # Attempt to set cookies directly or click buttons via JS
                        driver.execute_script("""
                            // Try to set consent cookies directly
                            document.cookie = "CONSENT=YES+; expires=Thu, 01 Jan 2030 00:00:00 UTC; path=/;";
                            
                            // Try to find and click any accept buttons
                            var buttons = document.querySelectorAll('button, input[type="button"], a.button');
                            for (var i = 0; i < buttons.length; i++) {
                                var button = buttons[i];
                                var text = button.textContent.toLowerCase();
                                if (text.includes('accept') || text.includes('agree') || 
                                    text.includes('consent') || text.includes('continue')) {
                                    button.click();
                                    return true;
                                }
                            }
                            return false;
                        """)
                        self.logger.info("Attempted JavaScript consent bypass")
                        time.sleep(1.5)
                        handled = True
                    except Exception as js_err:
                        self.logger.warning(f"JavaScript consent bypass failed: {js_err}")

            # Always check for cookie banners, even if not on a full consent page
            if not handled: # Only check if consent wasn't already handled
                if self._try_click_elements(driver, self.cookie_banner_selectors, "cookie banner button"):
                    self.logger.info("Handled a cookie banner.")
                    if take_screenshot and debug_dir:
                         self._take_consent_screenshot(driver, debug_dir, "cookie_banner")
                    time.sleep(random.uniform(0.5, 1.0))
                    handled = True # Indicate a banner was handled

            return handled # Return True if any consent/banner was clicked

        except Exception as e:
            self.logger.error(f"Error in consent handling: {e}", exc_info=True)
            return False # Return False on error

    def _try_click_elements(self, driver, selectors, description):
        """Try clicking elements matching a list of selectors (XPath or CSS)."""
        for selector in selectors:
            try:
                # Determine if selector is XPath or CSS
                find_method = By.XPATH if selector.startswith("/") or selector.startswith("(") else By.CSS_SELECTOR

                # Use WebDriverWait for potentially dynamic elements
                try:
                    elements = WebDriverWait(driver, 2).until(
                        EC.presence_of_all_elements_located((find_method, selector))
                    )
                except TimeoutException:
                    continue # Selector not found quickly, try next

                for element in elements:
                    # Check if element is visible and clickable
                    if element.is_displayed() and element.is_enabled():
                        try:
                            # Scroll element into view before clicking
                            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                            time.sleep(0.2)  # Short pause after scrolling
                            
                            # Try direct click with ActionChains (more reliable)
                            actions = ActionChains(driver)
                            actions.move_to_element(element).click().perform()
                            self.logger.info(f"Clicked {description} using ActionChains + selector: {selector}")
                            return True
                            
                        except StaleElementReferenceException:
                            self.logger.debug(f"Stale element reference for {description} selector: {selector}. Retrying find.")
                            break # Break inner loop to re-find elements
                            
                        except Exception as click_err:
                            self.logger.debug(f"Could not click {description} '{selector}' with ActionChains: {click_err}. Trying regular click.")
                            # Try regular click
                            try:
                                element.click()
                                self.logger.info(f"Clicked {description} using regular click: {selector}")
                                return True
                            except Exception as regular_click_err:
                                self.logger.debug(f"Regular click also failed: {regular_click_err}. Trying JS click.")
                                
                                # Try JavaScript click as final fallback
                                try:
                                    driver.execute_script("arguments[0].click();", element)
                                    self.logger.info(f"Clicked {description} using JavaScript fallback: {selector}")
                                    return True # JS Click successful
                                except Exception as js_click_err:
                                    self.logger.debug(f"JS click also failed for {description} '{selector}': {js_click_err}")
                                    # Continue to the next element or selector
            except Exception as find_err:
                self.logger.debug(f"Error finding {description} with selector {selector}: {find_err}")
        return False # No element clicked successfully

    def _take_consent_screenshot(self, driver, debug_dir, prefix):
        """Takes a screenshot for debugging consent issues."""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        screenshot_path = Path(debug_dir) / f"{prefix}_{timestamp}.png"
        try:
            driver.save_screenshot(str(screenshot_path))
            self.logger.info(f"Saved consent debug screenshot to {screenshot_path}")
        except Exception as e:
            self.logger.warning(f"Error saving consent screenshot: {e}")


class GoogleMapsGridScraper:
    """Enhanced Google Maps Grid Scraper with multi-threading and advanced features"""
    def __init__(self, headless=True, max_workers=5, debug=False, cache_enabled=True,
                 proxy_list=None, browser_error_threshold=3, no_images=False,
                 user_data_dir=None, driver_path=None, chrome_binary=None):
        """Initialize the Enhanced Google Maps Grid Scraper"""
        ensure_directories_exist()
        self.session_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.logger, self.grid_logger, self.business_logger = setup_logging(self.session_id)

        self.logger.info(f"🚀 Setting up Enhanced Google Maps Grid Scraper v{VERSION}")
        self.logger.info(f"Session ID: {self.session_id}")

        self.debug = debug
        self.headless = headless
        self.max_workers = max_workers
        self.no_images = no_images # Affects screenshots and matplotlib plots

        self.browser_pool = BrowserPool(
            max_browsers=max_workers, headless=headless, proxy_list=proxy_list,
            debug=debug, browser_error_threshold=browser_error_threshold,
            user_data_dir=user_data_dir, driver_path=driver_path, chrome_binary=chrome_binary
        )
        self.consent_handler = ConsentHandler(self.logger)
        self.cache = DataCache(enabled=cache_enabled)

        self.debug_dir = self._ensure_dir("debug")
        self.results_dir = self._ensure_dir("results")
        self.temp_dir = self._ensure_dir("temp")
        self.grid_data_dir = self._ensure_dir("grid_data")
        self.reports_dir = self._ensure_dir("reports") # Ensure reports dir exists

        self.results = [] # Stores final business data dictionaries
        self.processed_links = set() # Stores maps_url of successfully processed businesses
        self.seen_businesses = {} # key: (name, address_part), value: index in self.results for deduplication
        self.grid = [] # Stores the list of grid cell dictionaries
        self.current_grid_cell = None # Note: Less reliable in parallel mode

        self.lock = threading.Lock() # Lock for shared resources (results, stats, seen_businesses, grid cell updates)

        self.stats = defaultdict(int) # Use defaultdict for easier stat updates
        self.stats["start_time"] = None # Keep specific start time

        # Default configuration, can be overridden by args or interactive mode
        self.config = {
            "extract_emails": True, 
            "deep_email_search": False, # Deep search disabled by default (slow)
            "extract_social": True, 
            "save_screenshots": debug and not no_images,
            "grid_size_meters": 250, 
            "scroll_attempts": 20, # Increased for better coverage
            "scroll_pause_time": 2.0, # Increased for more reliable loading
            "email_timeout": 25, # Timeout for loading website for email search
            "retry_on_empty": False, # Don't retry empty cells by default
            "expand_grid_areas": True, # Expand bounds slightly
            "max_results": None, # Will be set by scrape/resume
            "search_zoom_level": 16, # Default zoom for grid cell search (adjusted for better results)
            "extract_reviews": True, # New: extract snippets of reviews
            "extract_hours": True, # New: extract opening hours
            "randomize_delays": True, # New: add random delays to appear more human-like
            "max_retries_per_url": 2, # New: max retries for failed URL extractions
            "result_threshold_per_cell": 40, # Max results to extract from a single cell
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
        cache_key = f"bounds_{location.lower().replace(' ', '_')}" # Normalize cache key
        cached_bounds = self.cache.get(cache_key)
        if cached_bounds:
            self.logger.info(f"Using cached bounds for {location}")
            print("Using cached boundaries.")
            return cached_bounds

        browser_id = None
        try:
            browser_id = self.browser_pool.get_browser(timeout=30) # Shorter timeout for bounds check
            driver = self.browser_pool.get_driver(browser_id)
            if not driver: raise Exception("Failed to get driver for bounds check")

            # Use standard Google Maps URL
            driver.get("https://www.google.com/maps")
            
            # Add random delay to appear more human
            time.sleep(random.uniform(2, 4))
            
            # Handle consent page if it appears
            self.consent_handler.handle_consent(driver, self.debug, self.debug_dir)
            
            # Try multiple search techniques for more reliable results
            
            # Method 1: Direct search box input
            try:
                # Wait for search box with longer timeout
                search_box = WebDriverWait(driver, 15).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "input#searchboxinput, input[name='q'], input[aria-label*='Search']"))
                )
                
                # Clear field and type slowly like a human
                search_box.clear()
                
                # Type location with random pauses between characters
                for char in location:
                    search_box.send_keys(char)
                    time.sleep(random.uniform(0.01, 0.1))
                
                time.sleep(random.uniform(0.5, 1.0))  # Pause before hitting enter
                search_box.send_keys(Keys.ENTER)
                
                self.logger.info(f"Searched for location: {location}")
                
                # Wait for the URL to update with coordinates
                WebDriverWait(driver, 20).until(
                    EC.url_contains("@")
                )
                self.logger.info(f"URL updated: {driver.current_url}")
                
            except TimeoutException:
                self.logger.warning("Timeout waiting for search box. Trying direct URL method.")
                
                # Method 2: Try direct URL with encoded query
                try:
                    encoded_location = quote(location)
                    direct_url = f"https://www.google.com/maps/search/{encoded_location}"
                    driver.get(direct_url)
                    
                    # Wait for map to load and URL to contain coordinates
                    WebDriverWait(driver, 20).until(
                        EC.url_contains("@")
                    )
                    self.logger.info(f"Direct URL method successful: {driver.current_url}")
                    
                except Exception as direct_url_err:
                    self.logger.error(f"Direct URL method failed: {direct_url_err}")
                    raise Exception("Could not perform location search")
                    
            except Exception as search_err:
                 self.logger.error(f"Error during location search input: {search_err}")
                 raise

            # Allow map to settle after URL update
            time.sleep(random.uniform(3, 5))

            if self.debug and not self.no_images:
                screenshot_path = self.debug_dir / f"location_search_{self.session_id}.png"
                try: driver.save_screenshot(str(screenshot_path))
                except Exception as e: self.logger.warning(f"Screenshot failed: {e}")

            # Try multiple times to get bounds via JS or URL
            bounds_data = None
            for attempt in range(3):
                try:
                    # Prioritize extracting from URL as it's often more reliable
                    current_url = driver.current_url
                    
                    # Try different URL patterns that Google Maps might use
                    coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+),(\d+\.?\d*)z', current_url)
                    if not coords_match:
                        coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+),(\d+\.?\d*)m/data', current_url)
                    
                    if coords_match:
                        lat = float(coords_match.group(1))
                        lng = float(coords_match.group(2))
                        zoom = float(coords_match.group(3))
                        # Estimate bounds based on zoom (adjust factors as needed)
                        # These factors are rough estimates and depend on latitude
                        lat_delta = 180 / math.pow(2, zoom + 1) # Smaller delta for better fit
                        lng_delta = 360 / math.pow(2, zoom + 1) * math.cos(math.radians(lat)) # Adjust for latitude

                        bounds_data = {
                            'northeast': {'lat': lat + lat_delta / 2, 'lng': lng + lng_delta / 2},
                            'southwest': {'lat': lat - lat_delta / 2, 'lng': lng - lng_delta / 2},
                            'center': {'lat': lat, 'lng': lng},
                            'zoom': zoom,
                            'method': 'url-estimation'
                        }
                        self.logger.info(f"Extracted bounds from URL (Attempt {attempt+1})")
                        break # Got bounds from URL

                    # Fallback: Try JS map bounds API (less reliable now)
                    # Enhanced JavaScript to detect more map object structures
                    bounds_data = driver.execute_script("""
                        try {
                            // Method 1: Find map element with standard Google Maps ID
                            let mapInstance;
                            const mapElement = document.getElementById('map');
                            if (mapElement && mapElement.__gm && mapElement.__gm.map) {
                                mapInstance = mapElement.__gm.map;
                            } 
                            // Method 2: Look for element with map data attribute
                            else if (document.querySelector('[data-map-id]')) {
                                const mapDataElement = document.querySelector('[data-map-id]');
                                if (mapDataElement.__gm && mapDataElement.__gm.map) {
                                    mapInstance = mapDataElement.__gm.map;
                                }
                            }
                            // Method 3: Find any element with __gm property containing map
                            else {
                                const maps = Array.from(document.querySelectorAll('*')).filter(el => 
                                    el.__gm && el.__gm.map && typeof el.__gm.map.getBounds === 'function');
                                if (maps.length > 0) mapInstance = maps[0].__gm.map;
                            }
                            
                            // If found a map instance, extract bounds
                            if (mapInstance && typeof mapInstance.getBounds === 'function') {
                                const bounds = mapInstance.getBounds();
                                const center = mapInstance.getCenter();
                                const zoom = mapInstance.getZoom();
                                
                                if (bounds && center && typeof zoom === 'number') {
                                    return {
                                        northeast: { 
                                            lat: bounds.getNorthEast().lat(), 
                                            lng: bounds.getNorthEast().lng() 
                                        },
                                        southwest: { 
                                            lat: bounds.getSouthWest().lat(), 
                                            lng: bounds.getSouthWest().lng() 
                                        },
                                        center: { 
                                            lat: center.lat(), 
                                            lng: center.lng() 
                                        },
                                        zoom: zoom,
                                        method: 'map-bounds-api'
                                    };
                                }
                            }
                            
                            // Method 4: Try to find map data in global variables
                            if (window.APP_INITIALIZATION_STATE) {
                                try {
                                    const jsonData = JSON.parse(window.APP_INITIALIZATION_STATE);
                                    if (jsonData && jsonData[1] && jsonData[1][0] && 
                                        jsonData[1][0][1] && jsonData[1][0][1][4]) {
                                        
                                        const mapData = jsonData[1][0][1][4];
                                        // Format varies, but often contains center and zoom
                                        if (Array.isArray(mapData) && mapData.length >= 3) {
                                            const centerLat = mapData[0];
                                            const centerLng = mapData[1];
                                            const zoom = mapData[2];
                                            
                                            if (typeof centerLat === 'number' && 
                                                typeof centerLng === 'number' &&
                                                typeof zoom === 'number') {
                                                
                                                // Estimate bounds based on zoom level
                                                const latDelta = 180 / Math.pow(2, zoom + 1);
                                                const lngDelta = 360 / Math.pow(2, zoom + 1) * 
                                                                Math.cos(centerLat * Math.PI / 180);
                                                
                                                return {
                                                    northeast: { 
                                                        lat: centerLat + latDelta/2, 
                                                        lng: centerLng + lngDelta/2 
                                                    },
                                                    southwest: { 
                                                        lat: centerLat - latDelta/2, 
                                                        lng: centerLng - lngDelta/2 
                                                    },
                                                    center: { 
                                                        lat: centerLat, 
                                                        lng: centerLng 
                                                    },
                                                    zoom: zoom,
                                                    method: 'app-initialization-state'
                                                };
                                            }
                                        }
                                    }
                                } catch (e) { /* Ignore parsing errors */ }
                            }
                            
                        } catch (e) { /* Ignore errors during JS execution */ }
                        return null;
                    """)
                    
                    if bounds_data:
                        self.logger.info(f"Extracted bounds via JS API (Attempt {attempt+1})")
                        break # Got bounds from JS

                except Exception as extract_err:
                    self.logger.warning(f"Bounds extraction attempt {attempt+1} failed: {extract_err}")
                time.sleep(2) # Wait before retrying

            if bounds_data:
                self.logger.info(f"Found city bounds: NE={bounds_data['northeast']}, SW={bounds_data['southwest']} (Method: {bounds_data.get('method', 'unknown')})")
                ne, sw = bounds_data['northeast'], bounds_data['southwest']
                lat_delta, lng_delta = abs(ne['lat'] - sw['lat']), abs(ne['lng'] - sw['lng'])
                avg_lat = (ne['lat'] + sw['lat']) / 2
                # More accurate distance calculation
                R = 6371 # Earth radius in km
                lat1, lon1 = math.radians(sw['lat']), math.radians(sw['lng'])
                lat2, lon2 = math.radians(ne['lat']), math.radians(ne['lng'])
                dlon = lon2 - lon1
                dlat = lat2 - lat1
                a_h = math.sin(dlat / 2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2)**2
                c_h = 2 * math.atan2(math.sqrt(a_h), math.sqrt(1 - a_h))
                width_km = R * abs(dlon) * math.cos(math.radians(avg_lat)) # Approx width
                height_km = R * abs(dlat) # Approx height

                self.logger.info(f"Approximate city size: {width_km:.2f}km x {height_km:.2f}km")
                print(f"City boundaries detected: ~{width_km:.1f}km x {height_km:.1f}km")

                # Expand bounds slightly (e.g., 5-10%) if enabled
                if self.config["expand_grid_areas"]:
                    expand_factor = 0.05 # 5% expansion on each side
                    center_lat = (ne['lat'] + sw['lat']) / 2
                    center_lng = (ne['lng'] + sw['lng']) / 2
                    expanded_lat_delta = lat_delta * (1 + expand_factor * 2)
                    expanded_lng_delta = lng_delta * (1 + expand_factor * 2)

                    expanded_bounds = {
                        'northeast': {'lat': center_lat + expanded_lat_delta / 2, 'lng': center_lng + expanded_lng_delta / 2},
                        'southwest': {'lat': center_lat - expanded_lat_delta / 2, 'lng': center_lng - expanded_lng_delta / 2},
                        'center': {'lat': center_lat, 'lng': center_lng},
                        'width_km': width_km, 'height_km': height_km, # Store original size estimate
                        'method': bounds_data.get('method', 'unknown')
                    }
                    self.logger.info(f"Expanded bounds by {expand_factor*100}%: NE={expanded_bounds['northeast']}, SW={expanded_bounds['southwest']}")
                    final_bounds = expanded_bounds
                else:
                    bounds_data['width_km'] = width_km
                    bounds_data['height_km'] = height_km
                    final_bounds = bounds_data

                self.cache.set(cache_key, final_bounds)
                return final_bounds
            else:
                self.logger.error("Could not determine city bounds after multiple attempts.")
                print("❌ Could not determine city boundaries.")
                return None
        except Exception as e:
            self.logger.error(f"Error getting city bounds: {e}", exc_info=self.debug)
            print(f"❌ Error getting city boundaries: {e}")
            if browser_id is not None: self.browser_pool.report_error(browser_id, "BoundsCheckError")
            return None # Return None on failure
        finally:
            if browser_id is not None:
                self.browser_pool.release_browser(browser_id)

    def create_optimal_grid(self, bounds, grid_size_meters=250):
        """Create an optimal grid based on city bounds"""
        self.logger.info(f"📊 Creating grid with {grid_size_meters}m cells")
        print(f"Creating grid with {grid_size_meters}m cells...")
        self.grid_logger.debug("=== STARTING GRID CREATION ===")

        ne_lat, ne_lng = bounds['northeast']['lat'], bounds['northeast']['lng']
        sw_lat, sw_lng = bounds['southwest']['lat'], bounds['southwest']['lng']

        # Use average latitude for meter-to-degree conversion
        avg_lat = (ne_lat + sw_lat) / 2
        meters_per_degree_lat = 111132.954 - 559.822 * math.cos(2 * math.radians(avg_lat)) + 1.175 * math.cos(4 * math.radians(avg_lat))
        meters_per_degree_lng = 111319.488 * math.cos(math.radians(avg_lat))

        # Avoid division by zero if meters_per_degree_lng is close to zero (near poles)
        if abs(meters_per_degree_lng) < 1e-6:
             self.logger.error("Cannot calculate grid near the poles (longitude convergence).")
             print("❌ Cannot create grid near the poles.")
             return None

        grid_size_lat = grid_size_meters / meters_per_degree_lat
        grid_size_lng = grid_size_meters / meters_per_degree_lng
        self.grid_logger.debug(f"Grid cell size (degrees): lat={grid_size_lat:.6f}, lng={grid_size_lng:.6f}")

        lat_span = abs(ne_lat - sw_lat)
        lng_span = abs(ne_lng - sw_lng)
        cells_lat = max(1, math.ceil(lat_span / grid_size_lat)) # Ensure at least 1 cell
        cells_lng = max(1, math.ceil(lng_span / grid_size_lng)) # Ensure at least 1 cell
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
                    "processed": False, # Initial state
                    "likely_empty": False, # Initial state
                    "business_count_estimate": 0 # Placeholder for potential future density estimation
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
        try:
            self._generate_html_visualization(grid, cells_lat, cells_lng, initial=True) # Generate initial HTML viz
        except Exception as html_viz_err:
             self.logger.warning(f"Initial HTML grid viz failed: {html_viz_err}")

        self.grid = grid
        self.stats["grid_cells_total"] = total_cells
        return grid


    def generate_grid_visualization(self, grid, rows, cols):
        """Generate a visual representation of the grid using Matplotlib"""
        if not MATPLOTLIB_AVAILABLE or self.no_images: return
        self.grid_logger.debug("\nGenerating Matplotlib Grid Visualization...")
        try:
            fig, ax = plt.subplots(figsize=(max(10, cols/5), max(8, rows/5)))
            for cell in grid:
                sw, ne = cell["southwest"], cell["northeast"]
                width, height = abs(ne["lng"] - sw["lng"]), abs(ne["lat"] - sw["lat"])
                color = 'lightgrey' # Default
                if cell.get("processed"):
                    color = '#e0ffe0' # Light green for processed
                    if cell.get("likely_empty"):
                        color = '#f0f0f0' # Lighter grey for processed empty
                rect = plt.Rectangle((sw["lng"], sw["lat"]), width, height,
                                     fill=True, color=color, edgecolor='blue', linewidth=0.2)
                ax.add_patch(rect)
                # Optionally add cell ID text for smaller grids
                if rows * cols < 500: # Only label small grids
                    ax.text(cell["center"]["lng"], cell["center"]["lat"], cell["cell_id"],
                            ha='center', va='center', fontsize=6, color='black')

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

            grid_viz_path = self.reports_dir / f"grid_visualization_{self.session_id}.png" # Save in reports
            plt.savefig(grid_viz_path, dpi=150)
            self.logger.info(f"Saved grid visualization to {grid_viz_path}")
            plt.close(fig)
        except Exception as e:
            self.logger.error(f"Error creating grid visualization: {e}", exc_info=True)


    def _generate_html_visualization(self, grid, rows, cols, initial=False):
        """Generate an HTML visualization of the grid"""
        if not grid: return
        try:
            # Determine filename based on whether it's the initial creation or an update
            filename_prefix = "grid_definition" if initial else "grid_progress"
            html_viz_path = self.grid_data_dir / f"{filename_prefix}_{self.session_id}.html"

            # Calculate progress if not initial
            progress_percent = 0
            if not initial:
                total_cells = self.stats["grid_cells_total"]
                processed_cells = self.stats["grid_cells_processed"]
                progress_percent = int(100 * processed_cells / max(1, total_cells))

            html_output = f"""
            <!DOCTYPE html><html><head><title>Grid {'Definition' if initial else 'Progress'}</title>
            {'<meta http-equiv="refresh" content="30">' if not initial else ''}
            <style>
            body {{ font-family: sans-serif; margin: 10px; }}
            .grid-container {{ overflow-x: auto; max-width: 100%; }}
            .grid {{ display: table; border-collapse: collapse; margin: 10px 0; min-width: {max(500, cols * 45)}px; }} /* Ensure minimum width */
            .row {{ display: table-row; }}
            .cell {{ display: table-cell; border: 1px solid #ccc; min-width: 40px; height: 20px;
                     text-align: center; vertical-align: middle; font-size: 9px; padding: 1px;
                     background-color: white; /* Default: Not Processed */ }}
            .cell.processed {{ background-color: #e0ffe0; }} /* Processed */
            .cell.processed.empty {{ background-color: #f0f0f0; }} /* Processed (Empty) */
            .header {{ font-weight: bold; background-color: #eee; position: sticky; top: 0; z-index: 1; }} /* Sticky header */
            .row-header {{ font-weight: bold; background-color: #eee; position: sticky; left: 0; z-index: 1; }} /* Sticky row header */
            .stats, .legend {{ margin: 10px; padding: 10px; border: 1px solid #ccc; font-size: 14px; background-color: #f9f9f9; border-radius: 5px; }}
            .legend-item {{ margin: 3px; display: inline-block; margin-right: 15px; }}
            .legend-box {{ display: inline-block; width: 15px; height: 15px; margin-right: 5px; vertical-align: middle; border: 1px solid #aaa; }}
            .progress-bar-container {{ width: 90%; background-color: #e0e0e0; border-radius: 4px; margin: 10px 0; height: 20px; overflow: hidden; }}
            .progress-bar {{ height: 100%; background-color: #4CAF50; border-radius: 4px; text-align: center; color: white; line-height: 20px; white-space: nowrap; transition: width 0.5s ease-in-out; }}
            </style></head><body>
            <h1>Grid {'Definition' if initial else 'Progress'}</h1>
            """

            if not initial:
                html_output += f"""
                <div class="stats"><h2>Scraping Statistics</h2>
                <p>Total Cells: {self.stats["grid_cells_total"]}</p><p>Processed Cells: {self.stats["grid_cells_processed"]}</p>
                <div class="progress-bar-container"><div class="progress-bar" style="width: {progress_percent}%">{progress_percent}%</div></div>
                <p>Empty Cells Found: {self.stats["grid_cells_empty"]}</p><p>Businesses Found: {self.stats["businesses_found"]}</p>
                <p>Emails Found: {self.stats["email_found_count"]}</p><p>Time Elapsed: {self.get_elapsed_time()}</p>
                </div>"""

            html_output += """
            <div class="legend">Legend:
            <div class="legend-item"><div class="legend-box" style="background-color: white;"></div> Not Processed</div>
            <div class="legend-item"><div class="legend-box" style="background-color: #e0ffe0;"></div> Processed</div>
            <div class="legend-item"><div class="legend-box" style="background-color: #f0f0f0;"></div> Processed (Empty)</div>
            </div>
            <div class="grid-container"><div class="grid">
            """

            # Headers
            html_output += "<div class='row'><div class='cell header row-header'>R\\C</div>" # Top-left corner
            for c in range(cols): html_output += f"<div class='cell header'>{c}</div>"
            html_output += "</div>"

            # Rows
            cell_status = {cell["cell_id"]: cell for cell in grid} # Create lookup map
            for r in range(rows):
                html_output += f"<div class='row'><div class='cell header row-header'>{r}</div>" # Row header
                for c in range(cols):
                    cell_id = f"r{r}c{c}"
                    cell_data = cell_status.get(cell_id)
                    cell_class = "cell"
                    if cell_data and cell_data.get("processed"):
                        cell_class += " processed"
                        if cell_data.get("likely_empty"):
                            cell_class += " empty"
                    html_output += f"<div class='{cell_class}' title='{cell_id}'>{cell_id}</div>"
                html_output += "</div>"

            html_output += "</div></div></body></html>" # Close grid, grid-container, body, html

            with open(html_viz_path, "w", encoding='utf-8') as f: f.write(html_output)
            self.logger.info(f"Saved HTML grid visualization to {html_viz_path}")
        except Exception as e:
            self.logger.warning(f"Error creating HTML visualization: {e}", exc_info=True)


    def update_grid_visualization(self):
        """Update the HTML grid visualization with current progress"""
        if not self.grid: return
        try:
            rows = max(cell["row"] for cell in self.grid) + 1
            cols = max(cell["col"] for cell in self.grid) + 1
            # Generate the HTML using the helper function, indicating it's an update
            self._generate_html_visualization(self.grid, rows, cols, initial=False)
        except Exception as e:
            self.logger.warning(f"Error updating grid visualization: {e}", exc_info=True)


    def get_elapsed_time(self):
        """Get elapsed time in human-readable format"""
        if not self.stats["start_time"]: return "00:00:00"
        try:
            elapsed_seconds = (datetime.now() - self.stats["start_time"]).total_seconds()
            hours, remainder = divmod(int(elapsed_seconds), 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except TypeError: # Handle case where start_time might not be a datetime object initially
             return "00:00:00"

    def search_in_grid_cell(self, query, grid_cell):
        """Search for places in a specific grid cell. Returns list of links. Marks cell as processed."""
        cell_id = grid_cell["cell_id"]
        center = grid_cell["center"]
        thread_id = threading.get_ident() # Identify thread for logging
        self.logger.debug(f"Thread {thread_id} starting search in grid cell {cell_id} @ {center['lat']:.5f},{center['lng']:.5f}")

        browser_id = None # Initialize browser_id
        attempts = 0
        max_attempts = self.config.get("max_retries_per_url", 2) # Get from config

        while attempts < max_attempts:
            attempts += 1
            self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Search attempt {attempts}/{max_attempts}")
            try:
                browser_id = self.browser_pool.get_browser()
                driver = self.browser_pool.get_driver(browser_id)
                if not driver:
                    raise Exception(f"Failed to get driver for cell {cell_id}")

                # Construct search URL using standard Google Maps search
                # Use a specific zoom level based on config
                zoom_level = self.config.get("search_zoom_level", 16)
                # URL encode the query
                encoded_query = quote(query)
                # Construct the URL
                url = f"https://www.google.com/maps/search/{encoded_query}/@{center['lat']:.7f},{center['lng']:.7f},{zoom_level}z/data=!3m1!4b1?entry=ttu"

                self.logger.info(f"Thread {thread_id} - Cell {cell_id} URL (Attempt {attempts}): {url}")

                driver.get(url)
                
                # Randomized human-like waiting pattern
                wait_time = random.uniform(2, 4) if self.config.get("randomize_delays", True) else 3
                time.sleep(wait_time) # Wait for initial load

                # Handle consent/login immediately after loading
                if self.consent_handler.handle_consent(driver, self.debug, self.debug_dir):
                    self.stats["consent_pages_handled"] += 1
                    time.sleep(random.uniform(1, 2)) # Extra wait after consent handling

                # Check if the page loaded correctly (e.g., still on maps domain, check for results panel)
                current_url = driver.current_url
                if "google.com/maps/search" not in current_url and "google.com/maps/place" not in current_url:
                    self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Redirected from search results page to: {current_url}. Retrying if possible.")
                    if attempts < max_attempts:
                         self.browser_pool.report_error(browser_id, "RedirectError") # Report error, might trigger recreate
                         self.browser_pool.release_browser(browser_id) # Release before retry
                         browser_id = None # Reset browser_id for next attempt
                         time.sleep(random.uniform(3, 5)) # Wait before retrying
                         continue # Go to next attempt
                    else:
                        self.logger.error(f"Thread {thread_id} - Cell {cell_id} - Failed to load search results after {max_attempts} attempts (URL: {current_url}). Skipping cell.")
                        # Mark cell as processed but problematic
                        with self.lock:
                            grid_cell["processed"] = True
                            self.stats["grid_cells_processed"] += 1
                            self.stats["extraction_errors"] += 1 # Count as an error
                        return [] # Return empty list

                # Wait for results feed/panel to appear with improved selectors
                try:
                    # Try multiple selector strategies
                    for selector in [
                        RESULTS_PANEL_SELECTOR,  # Primary selector
                        "div[role='feed']",      # Common feed selector
                        "div.m6QErb",            # Alternative container class
                        "div[data-result-index]", # Results with indices
                        "div#search-views",      # Search container
                    ]:
                        try:
                            element = WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            if element:
                                self.logger.debug(f"Results panel found with selector: {selector}")
                                break
                        except TimeoutException:
                            continue
                    
                    # If we get here without finding a panel, one more attempt with a longer timeout
                    if not element:
                        self.logger.debug("Trying longer timeout for results panel...")
                        element = WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, RESULTS_PANEL_SELECTOR))
                        )
                    
                    self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Results feed loaded.")
                except TimeoutException:
                    # Check for "No results found" message
                    try:
                        no_results_elements = driver.find_elements(By.XPATH, END_OF_RESULTS_XPATH)
                        if no_results_elements and any(el.is_displayed() for el in no_results_elements):
                            self.logger.info(f"Thread {thread_id} - Cell {cell_id} - Explicitly found 'No results found'.")
                            with self.lock:
                                grid_cell["processed"] = True
                                grid_cell["likely_empty"] = True
                                self.stats["grid_cells_processed"] += 1
                                self.stats["grid_cells_empty"] += 1
                            return [] # Success, but no results
                    except Exception as no_results_err:
                        self.logger.debug(f"Error checking for 'no results' message: {no_results_err}")
                    
                    # Also check if we see any place links even if the primary container isn't found
                    try:
                        place_links = driver.find_elements(By.CSS_SELECTOR, RESULT_LINK_SELECTOR)
                        if place_links:
                            self.logger.info(f"Thread {thread_id} - Cell {cell_id} - Found {len(place_links)} place links despite missing main container.")
                            # Continue processing even without the main container
                        else:
                            self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Timeout waiting for search results feed, and no place links found. (URL: {driver.current_url})")
                            if self.debug and not self.no_images:
                                screenshot_path = self.debug_dir / f"no_feed_{cell_id}_{self.session_id}.png"
                                try: driver.save_screenshot(str(screenshot_path))
                                except Exception as e: self.logger.warning(f"Screenshot failed: {e}")

                            if attempts < max_attempts:
                                self.browser_pool.report_error(browser_id, "NoFeedError")
                                self.browser_pool.release_browser(browser_id)
                                browser_id = None
                                time.sleep(random.uniform(3, 5))
                                continue # Go to next attempt
                            else:
                                self.logger.error(f"Thread {thread_id} - Cell {cell_id} - Failed to find feed after {max_attempts} attempts. Skipping cell.")
                                with self.lock:
                                    grid_cell["processed"] = True
                                    self.stats["grid_cells_processed"] += 1
                                    self.stats["extraction_errors"] += 1
                                return []
                    except Exception as links_err:
                        self.logger.warning(f"Error checking for place links: {links_err}")
                        # Continue with retry logic for feed missing

                # Take screenshot if debugging (randomly)
                if self.debug and not self.no_images and random.random() < 0.05: # Screenshot 5% of cells
                    screenshot_path = self.debug_dir / f"grid_cell_{cell_id}_{self.session_id}.png"
                    try: driver.save_screenshot(str(screenshot_path))
                    except Exception as e: self.logger.warning(f"Screenshot failed for {cell_id}: {e}")

                # --- Link Extraction ---
                business_links = set() # Use a set for automatic deduplication

                # Scroll and collect links
                try:
                    scroll_links = self.scroll_and_collect_links(driver, max_scrolls=self.config["scroll_attempts"])
                    if scroll_links: business_links.update(scroll_links)
                    self.logger.debug(f"Thread {thread_id} - Cell {cell_id} - Found {len(business_links)} total unique links after scrolling.")
                except Exception as e:
                    self.logger.warning(f"Thread {thread_id} - Cell {cell_id} - Error scrolling/collecting links: {e}", exc_info=self.debug)
                    # Proceed with any links found before the error

                business_links_list = list(business_links)

                # --- Update Cell Status and Stats ---
                with self.lock: # Lock for updating shared grid state and stats
                    if not grid_cell.get("processed"): # Ensure we only count processing once per cell lifecycle
                        self.stats["grid_cells_processed"] += 1
                    grid_cell["processed"] = True # Mark as processed

                    if not business_links_list:
                        self.logger.info(f"Thread {thread_id} - Cell {cell_id} - No business links found after search and scroll.")
                        grid_cell["likely_empty"] = True
                        self.stats["grid_cells_empty"] += 1 # Increment only if it wasn't already marked empty
                    else:
                        self.logger.info(f"Thread {thread_id} - Cell {cell_id} - Found {len(business_links_list)} unique business links.")
                        grid_cell["likely_empty"] = False # Mark as not empty if links found

                # Save links to temp file (useful for debugging/recovery)
                if business_links_list:
                    try:
                        links_file = self.temp_dir / f"cell_{cell_id}_links_{self.session_id}.json"
                        with open(links_file, "w", encoding='utf-8') as f:
                            json.dump(business_links_list, f, indent=2)
                    except Exception as e:
                        self.logger.warning(f"Error saving links temp file for {cell_id}: {e}")

                return business_links_list # Success for this attempt

            except Exception as e:
                self.logger.error(f"Thread {thread_id} - Error searching in grid cell {cell_id} (Attempt {attempts}): {e}", exc_info=self.debug)
                if browser_id is not None:
                    self.browser_pool.report_error(browser_id, "SearchError")
                if attempts >= max_attempts:
                     self.logger.error(f"Thread {thread_id} - Cell {cell_id} - Failed search after {max_attempts} attempts. Skipping cell.")
                     # Mark cell as processed with error
                     with self.lock:
                         if not grid_cell.get("processed"):
                             self.stats["grid_cells_processed"] += 1
                         grid_cell["processed"] = True
                         self.stats["extraction_errors"] += 1 # Count as error
                     return [] # Return empty list on final failure
                else:
                    # Wait before retrying
                    time.sleep(random.uniform(3, 5))

            finally:
                if browser_id is not None:
                    self.browser_pool.release_browser(browser_id)
                    browser_id = None # Reset for next loop iteration if retrying

        # Should not be reached if logic is correct, but as a fallback:
        self.logger.error(f"Thread {thread_id} - Cell {cell_id} - Exited search loop unexpectedly.")
        with self.lock:
             if not grid_cell.get("processed"): self.stats["grid_cells_processed"] += 1
             grid_cell["processed"] = True
             self.stats["extraction_errors"] += 1
        return []


    def extract_visible_links(self, driver):
        """Extract visible business links without scrolling using JS"""
        self.logger.debug("Extracting initially visible links...")
        try:
            # Improved resilient JS extraction with multiple strategies
            links = driver.execute_script(f"""
                const links = new Set();
                
                // Strategy 1: Find result items first, then links within them
                try {{
                    const resultSelectors = [
                        '{RESULT_ITEM_SELECTOR}',
                        'div[jsaction*="mouseover"]',
                        'div[data-result-index]',
                        'div.Nv2PK', 
                        'div.lI9IFe'
                    ];
                    
                    for (const selector of resultSelectors) {{
                        const resultItems = document.querySelectorAll(selector);
                        for (const item of resultItems) {{
                            // Find link inside the result item
                            const linkElement = item.querySelector('a[href*="/maps/place/"]');
                            if (linkElement && linkElement.href && 
                                linkElement.href.includes('/maps/place/') && 
                                linkElement.href.includes('@')) {{
                                links.add(linkElement.href);
                            }}
                            // If the item itself is a link
                            else if (item.tagName === 'A' && item.href && 
                                    item.href.includes('/maps/place/') && 
                                    item.href.includes('@')) {{
                                links.add(item.href);
                            }}
                        }}
                    }}
                }} catch (e) {{ console.error('Error in Strategy 1:', e); }}
                
                // Strategy 2: Search globally within results container
                if (links.size === 0) {{
                    try {{
                        const containerSelectors = [
                            '{RESULTS_PANEL_SELECTOR}',
                            'div[role="feed"]',
                            'div.m6QErb',
                            'div#search-views',
                            'div[role="main"]'
                        ];
                        
                        for (const containerSelector of containerSelectors) {{
                            const containers = document.querySelectorAll(containerSelector);
                            for (const container of containers) {{
                                const linkElements = container.querySelectorAll('a[href*="/maps/place/"]');
                                for (const link of linkElements) {{
                                    if (link.href && link.href.includes('/maps/place/') && link.href.includes('@')) {{
                                        links.add(link.href);
                                    }}
                                }}
                            }}
                        }}
                    }}

                    } catch (e) { console.error('Error in Strategy 2:', e); }
                }
                
                // Strategy 3: Fallback to search entire document
                if (links.size === 0) {
                    try {
                        document.querySelectorAll('a[href*="/maps/place/"]').forEach(el => {
                            if (el.href && el.href.includes('/maps/place/') && el.href.includes('@')) {
                                links.add(el.href);
                            }
                        });
                    } catch (e) { console.error('Error in Strategy 3:', e); }
                }
                
                // Clean and filter links
                const filteredLinks = Array.from(links).filter(url => {
                    // Filter out links that don't have the expected structure
                    return url.includes('/maps/place/') && url.includes('@') &&
                           !url.includes('image?') && !url.includes('/image');
                });
                
                return filteredLinks;
            """)
            self.logger.debug(f"Found {len(links) if links else 0} initially visible links.")
            return links if links else []
        except Exception as e:
            self.logger.warning(f"Error extracting visible links via JS: {e}")
            # Fallback to Selenium find_elements (slower)
            try:
                # Try different selectors in case the primary one fails
                selectors = [
                    f"{RESULTS_PANEL_SELECTOR} {RESULT_LINK_SELECTOR}",
                    f"{RESULT_LINK_SELECTOR}",
                    "div[role='feed'] a[href*='/maps/place/']",
                    "div.m6QErb a[href*='/maps/place/']"
                ]
                
                selenium_links = set()
                for selector in selectors:
                    try:
                        link_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                        for el in link_elements:
                            href = el.get_attribute('href')
                            if href and "/maps/place/" in href and "/@" in href:
                                selenium_links.add(href)
                    except Exception as selector_err:
                        self.logger.debug(f"Error with selector {selector}: {selector_err}")
                
                self.logger.debug(f"Found {len(selenium_links)} links via Selenium fallback.")
                return list(selenium_links)
            except Exception as se:
                 self.logger.warning(f"Selenium link extraction fallback failed: {se}")
                 return []


    def scroll_and_collect_links(self, driver, max_scrolls=20):
        """Scroll through results and collect business links with improved reliability"""
        links_found = set()
        stagnant_scroll_count = 0 # Counts consecutive scrolls with no height change
        stagnant_link_count = 0 # Counts consecutive scrolls with no new links found
        scroll_element = None
        scroll_target_description = "window" # Default target

        # --- Find the scrollable element with improved selectors and error handling ---
        try:
            # Try multiple selectors to find the scrollable container
            scroll_selectors = [
                RESULTS_PANEL_SELECTOR,  # Main selector combining multiple options
                "div[role='feed']",      # Most common feed container
                "div.m6QErb[role='region']", # Region container
                "div.siAUzd-neVct", # New container class
                "div.DxyBCb"        # Older container class
            ]
            
            for selector in scroll_selectors:
                try:
                    elements = driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        # Find element with largest scroll height
                        best_element = None
                        max_scroll = -1
                        for el in elements:
                            try:
                                sh = driver.execute_script("return arguments[0].scrollHeight", el)
                                if sh > max_scroll:
                                    max_scroll = sh
                                    best_element = el
                            except Exception:
                                continue
                        
                        if best_element:
                            scroll_element = best_element
                            scroll_target_description = f"'{selector}' (h:{max_scroll})"
                            self.logger.debug(f"Found scrollable container: {scroll_target_description}")
                            break
                except Exception as selector_err:
                    self.logger.debug(f"Error finding scroll container with selector {selector}: {selector_err}")
            
            # If no scroll element found but feed seems present, try JavaScript to find it
            if not scroll_element:
                try:
                    js_scroll_element = driver.execute_script("""
                        // Find most likely scroll container
                        const containers = [
                            document.querySelector('div[role="feed"]'),
                            document.querySelector('div.m6QErb[role="region"]'),
                            document.querySelector('div.DxyBCb'),
                            document.querySelector('div.siAUzd-neVct'),
                            document.querySelector('div[jsaction*="mouseover"]')
                        ].filter(el => el !== null);
                        
                        // Find the container with greatest scrollHeight
                        let bestContainer = null;
                        let maxHeight = 0;
                        for (const container of containers) {
                            if (container.scrollHeight > maxHeight) {
                                maxHeight = container.scrollHeight;
                                bestContainer = container;
                            }
                        }
                        
                        return bestContainer;
                    """)
                    if js_scroll_element:
                        scroll_element = js_scroll_element
                        scroll_target_description = "JS-identified container"
                        self.logger.debug("Found scrollable container via JavaScript fallback")
                except Exception as js_err:
                    self.logger.debug(f"JavaScript scroll container detection failed: {js_err}")
        except Exception as find_err:
            self.logger.warning(f"Error identifying scroll container: {find_err}")

        if not scroll_element:
            self.logger.warning("Could not find specific scrollable feed container, falling back to scrolling window/body.")
            scroll_element = None # Explicitly set to None to use window scroll
            scroll_target_description = "window"

        # --- Scrolling Loop ---
        last_scroll_height = 0
        scroll_pause = self.config["scroll_pause_time"]
        result_threshold = self.config.get("result_threshold_per_cell", 40)

        # Define the JS function to extract links
        js_script = """
            const itemSelector = arguments[0];
            const linkSelector = arguments[1];
            const parentSelectors = arguments[2]; // List of parent selectors
            const links = new Set();

            // Try finding links within result items first
            try {
                document.querySelectorAll(itemSelector).forEach(item => {
                    // Find link within the item
                    const linkElement = item.querySelector(linkSelector);
                    if (linkElement && linkElement.href && 
                        linkElement.href.includes('/maps/place/') && 
                        linkElement.href.includes('@')) {
                        links.add(linkElement.href);
                    }
                    // Check if the item itself is a link
                    else if (item.tagName === 'A' && item.href && 
                            item.href.includes('/maps/place/') && 
                            item.href.includes('@')) {
                        links.add(item.href);
                    }
                });
            } catch (e) { console.warn('Error querying itemSelector:', e); }

            // Fallback: If few links found via itemSelector, try querying within parent selectors
            if (links.size < 5) {
                parentSelectors.forEach(parentSel => {
                    try {
                        document.querySelectorAll(parentSel + ' ' + linkSelector).forEach(el => {
                            if (el.href && el.href.includes('/maps/place/') && el.href.includes('@')) {
                                links.add(el.href);
                            }
                        });
                    } catch (e) { console.warn('Error querying parentSelector:', parentSel, e); }
                });
            }
            
            // Last resort: search entire document if still not enough links
            if (links.size < 3) {
                try {
                    document.querySelectorAll('a[href*="/maps/place/"]').forEach(el => {
                        if (el.href && el.href.includes('/maps/place/') && 
                            el.href.includes('@') && !el.href.includes('image?')) {
                            links.add(el.href);
                        }
                    });
                } catch (e) { console.warn('Error in global search:', e); }
            }
            
            return Array.from(links);
        """
        
        # Improved selectors for finding results
        item_selectors = [
            RESULT_ITEM_SELECTOR,
            "div[jsaction*='mouseover'], div.Nv2PK, a[href*='/maps/place/']",
            "div[data-result-index]"
        ]
        
        link_selectors = [
            RESULT_LINK_SELECTOR,
            "a[href*='/maps/place/']"
        ]
        
        # Combine primary and fallback selectors for the JS argument
        all_parent_selectors = [
            RESULTS_PANEL_SELECTOR,
            "div[role='feed']", 
            "div.m6QErb", 
            "div.DxyBCb",
            "div[role='region']",
            "div[role='main']"
        ]

        # First extract links visible without scrolling
        try:
            initial_links = self.extract_visible_links(driver)
            if initial_links:
                links_found.update(initial_links)
                self.logger.debug(f"Found {len(links_found)} links before scrolling.")
        except Exception as initial_extract_err:
            self.logger.warning(f"Error extracting initial links: {initial_extract_err}")

        # Begin scrolling loop with improved error handling
        for i in range(max_scrolls):
            initial_link_count = len(links_found)

            # Extract links *before* scrolling (to catch links loaded by previous scroll)
            try:
                # Try different item selectors for more comprehensive coverage
                for item_selector in item_selectors:
                    for link_selector in link_selectors:
                        try:
                            # Execute the JS script with selectors as arguments
                            new_links = driver.execute_script(js_script, item_selector, link_selector, all_parent_selectors)
                            if new_links:
                                links_found.update(new_links)
                        except Exception as js_err:
                            self.logger.debug(f"JS extraction failed with selectors {item_selector}, {link_selector}: {js_err}")
                
                self.logger.debug(f"Scroll iter {i+1}: Found {len(links_found)} total links so far.")
            except Exception as extract_err:
                self.logger.warning(f"Error extracting links during scroll iter {i+1}: {extract_err}")

            # Check early exit conditions
            if len(links_found) >= result_threshold:
                self.logger.info(f"Reached result threshold ({result_threshold}) after {i+1} scrolls. Stopping.")
                break

            # Check if new links were found in this iteration
            if len(links_found) == initial_link_count and i > 0: # Don't count first iteration
                stagnant_link_count += 1
                self.logger.debug(f"No new links found in scroll iter {i+1} (stagnant link count: {stagnant_link_count})")
            else:
                stagnant_link_count = 0 # Reset if new links were found

            # Check for "end of results" message
            try:
                end_markers = driver.find_elements(By.XPATH, END_OF_RESULTS_XPATH)
                if any(el.is_displayed() for el in end_markers): # Check if any are visible
                    self.logger.info(f"Reached end of results marker after scroll {i+1}.")
                    break # Exit scroll loop
            except Exception as end_marker_err:
                 self.logger.debug(f"Error checking for end of results marker: {end_marker_err}")

            # Scroll down with enhanced reliability
            current_scroll_height = -1
            try:
                if scroll_element:
                    try:
                        # Use more resilient scrolling approach
                        last_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element)
                        
                        # Scroll to specific positions within the container for more reliable loading
                        if i % 3 == 0:  # Every third scroll, go to middle then bottom
                            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight / 2", scroll_element)
                            time.sleep(scroll_pause / 3)
                            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_element)
                        else:  # Regular scroll to bottom
                            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_element)
                        
                        # Wait for scrolling animation and content loading
                        time.sleep(scroll_pause / 2)
                        
                        # Try to trigger content loading with small scroll adjustments
                        driver.execute_script("""
                            const el = arguments[0];
                            // Small up and down to trigger any lazy loading
                            el.scrollTop = el.scrollHeight - 10;
                            setTimeout(() => { el.scrollTop = el.scrollHeight; }, 100);
                        """, scroll_element)
                        
                        time.sleep(scroll_pause / 3)
                        current_scroll_height = driver.execute_script("return arguments[0].scrollHeight", scroll_element)
                    except Exception as specific_scroll_err:
                        self.logger.warning(f"Error during element scroll: {specific_scroll_err}")
                        # Fall back to window scrolling if element scrolling fails
                        scroll_element = None
                        scroll_target_description = "window (fallback)"
                else: # Scroll window with improved reliability
                    last_scroll_height = driver.execute_script("return document.body.scrollHeight")
                    
                    # Use smoother scrolling to trigger all dynamic loading
                    if i % 3 == 0:  # Every third scroll, use incremental scrolling
                        driver.execute_script("""
                            const totalHeight = document.body.scrollHeight;
                            const steps = 3;
                            for (let step = 1; step <= steps; step++) {
                                setTimeout(() => {
                                    window.scrollTo(0, (totalHeight * step) / steps);
                                }, step * 100);
                            }
                        """)
                    else:  # Regular scroll to bottom
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    
                    # Wait for scrolling and content loading
                    time.sleep(scroll_pause / 2)
                    
                    # Small adjustment to trigger any remaining lazy loading
                    driver.execute_script("""
                        window.scrollTo(0, document.body.scrollHeight - 10);
                        setTimeout(() => { window.scrollTo(0, document.body.scrollHeight); }, 100);
                    """)
                    
                    time.sleep(scroll_pause / 3)
                    current_scroll_height = driver.execute_script("return document.body.scrollHeight")

                self.logger.debug(f"Scrolled {scroll_target_description} (Iter {i+1}/{max_scrolls}). Height: {last_scroll_height} -> {current_scroll_height}")
                
                # Add a small random delay to appear more human-like
                if self.config.get("randomize_delays", True):
                    time.sleep(random.uniform(0, 0.5))
                else:
                    time.sleep(scroll_pause / 4)

                # Check if scroll height changed significantly
                if abs(current_scroll_height - last_scroll_height) < 50 and i > 0: # If height didn't change much
                    stagnant_scroll_count += 1
                    self.logger.debug(f"Scroll height stagnant ({stagnant_scroll_count}) at iteration {i+1}")
                else:
                    stagnant_scroll_count = 0 # Reset if height changed

            except Exception as scroll_err:
                self.logger.warning(f"Error during scroll iter {i+1}: {scroll_err}")
                stagnant_scroll_count += 1 # Count as stagnant if scroll fails

            # Break if stagnant for too long (either no new links or no scroll height change)
            # More tolerant stagnation checks
            if stagnant_scroll_count >= 4 or stagnant_link_count >= 4:
                self.logger.info(f"Scrolling stopped after {i+1} scrolls due to stagnant content (Scroll:{stagnant_scroll_count}, Link:{stagnant_link_count}).")
                break

        # Final link extraction pass after all scrolling is complete
        try:
            final_links = self.extract_visible_links(driver)
            if final_links:
                links_found.update(final_links)
        except Exception as final_extract_err:
            self.logger.warning(f"Error in final link extraction: {final_extract_err}")

        self.logger.info(f"Finished scrolling. Found {len(links_found)} total unique links.")
        return list(links_found)


    def extract_place_info(self, url, driver):
        """Extract business information from a Google Maps place URL with improved reliability"""
        thread_id = threading.get_ident() # Identify thread for logging

        # --- Pre-checks ---
        # Basic URL validation using regex
        if not url or not MAPS_URL_PATTERN.match(url):
            self.logger.warning(f"Thread {thread_id} - Skipping invalid/non-place URL: {url}")
            return None

        # Check processed links (read is generally safe without lock, but add uses lock)
        with self.lock:
            if url in self.processed_links:
                self.logger.debug(f"Thread {thread_id} - Skipping already processed URL: {url[:60]}...")
                self.stats["duplicates_skipped_early"] += 1
                return None

        # Check for known problematic patterns before loading
        if "sorry/index" in url or "consent" in url or "batchexecute" in url:
            with self.lock: self.stats["rate_limit_hits"] += 1
            self.logger.warning(f"Thread {thread_id} - Skipping likely rate limit/consent URL pattern: {url[:60]}...")
            return None

        self.logger.debug(f"Thread {thread_id} - Processing URL: {url[:80]}...")

        place_info = defaultdict(str) # Use defaultdict for easier assignments
        place_info.update({
            "maps_url": url,
            "social_links": {},
            "scrape_timestamp": datetime.now().isoformat(), # Use ISO format timestamp
            "place_id": self.extract_place_id(url),
            "category": "", # Initialize common fields
            "address": "",
            "phone": "",
            "website": "",
            "rating": "",
            "reviews_count": "",
            "email": "",
            "opening_hours": "", # New field
            "price_level": "",   # New field
            "reviews": [],       # New field for review snippets
            "popular_times": "", # New field
            "features": []       # New field for business features/attributes
        })

        try:
            # --- Load Page with improved error handling ---
            try:
                driver.get(url)
                
                # Add random delay to appear more human-like
                if self.config.get("randomize_delays", True):
                    time.sleep(random.uniform(2.0, 4.0))
                else:
                    time.sleep(3.0)
                    
                # Wait for the main heading or other key elements to be present
                try:
                    # Try multiple selectors to determine when page is ready
                    ready_selectors = [
                        "h1", 
                        "h1.DUwDvf", 
                        "h1[class*='header']", 
                        "div[role='main'] h1",
                        "button[data-item-id='address']",
                        "div.rogA2c",  # Common Maps business info container
                        "div.LBgpqf" # Another Maps container
                    ]
                    
                    # Wait for at least one selector to be present
                    for selector in ready_selectors:
                        try:
                            WebDriverWait(driver, 5).until(
                                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                            )
                            self.logger.debug(f"Page ready detected with selector: {selector}")
                            break
                        except TimeoutException:
                            continue
                except TimeoutException:
                    self.logger.warning(f"Thread {thread_id} - Timeout waiting for page elements on place page: {url[:80]}")
                    # Check for redirects or error states
                    current_url = driver.current_url
                    if "google.com/maps/search" in current_url and url != current_url:
                        self.logger.warning(f"Thread {thread_id} - Redirected back to search page from place URL: {url[:80]}... Skipping.")
                        return None
                    if "sorry/index" in current_url or "consent" in current_url:
                        self.logger.warning(f"Thread {thread_id} - Rate limited or consent required: {current_url}")
                        return None
                    # Continue anyway, as some elements might still be accessible
            
                # Handle consent/login if it appears on the place page
                if self.consent_handler.handle_consent(driver, self.debug, self.debug_dir):
                    self.stats["consent_pages_handled"] += 1
                    time.sleep(random.uniform(1, 2))

                # Additional check for rate limit / redirection after load
                current_page_url = driver.current_url
                if "sorry/index" in current_page_url or "consent" in current_page_url or "batchexecute" in current_page_url:
                    with self.lock: self.stats["rate_limit_hits"] += 1
                    self.logger.warning(f"Thread {thread_id} - Hit rate limit/consent page loading place: {url[:60]}...")
                    return None
                if "google.com/maps/search" in current_page_url and url != current_page_url: # If redirected back to search
                    self.logger.warning(f"Thread {thread_id} - Redirected back to search page from place URL: {url[:80]}...")
                    return None
                    
            except TimeoutException as to_err:
                self.logger.warning(f"Thread {thread_id} - Timeout loading place page: {url[:80]} - {to_err}")
                return None
            except Exception as load_err:
                self.logger.error(f"Thread {thread_id} - Error loading place page: {url[:80]} - {load_err}")
                return None

            # --- Extract Core Information ---
            # Enhanced JavaScript extraction first (more comprehensive)
            try:
                js_data = driver.execute_script(self._get_enhanced_js_extraction_script())
                if js_data:
                    # Update place_info with data from JS
                    for key, value in js_data.items():
                        if value and key in place_info:
                            place_info[key] = value
                        elif key == "features" and value:
                            place_info["features"] = value  # List of features/attributes
                        elif key == "reviews" and value:
                            place_info["reviews"] = value[:3]  # Store up to 3 review snippets
                    self.logger.debug(f"Thread {thread_id} - JS extracted data for {url[:60]}")
            except Exception as js_err:
                self.logger.warning(f"Thread {thread_id} - JS extraction failed for {url[:60]}: {js_err}")

            # --- Fallback/Supplement with Selenium Finders ---
            # Only extract with Selenium if we didn't get the data via JS
            self._selenium_fallback_extraction(driver, place_info)

            # If still no name, it's likely a failed load or weird page
            if not place_info["name"]:
                self.logger.warning(f"Thread {thread_id} - Could not extract name for URL: {url[:80]}... Skipping.")
                if self.debug and not self.no_images:
                     screenshot_path = self.debug_dir / f"no_name_{self.extract_place_id(url) or 'unknown_id'}_{self.session_id}.png"
                     try: driver.save_screenshot(str(screenshot_path))
                     except Exception as e: self.logger.warning(f"Screenshot failed: {e}")
                with self.lock: self.stats["extraction_errors"] += 1
                return None # Cannot proceed without a name

            # --- Additional Extractions ---
            # Coordinates (re-extract from current URL in case it updated)
            place_info["coordinates"] = self.extract_coordinates_from_url(driver.current_url)

            # Social Media Links
            if self.config["extract_social"]:
                try: 
                    social_links = self.extract_social_media_links(driver)
                    if social_links:
                        place_info["social_links"] = social_links
                except Exception as e: 
                    self.logger.warning(f"Social link extraction failed: {e}")

            # Email (only if website found and enabled)
            if place_info["website"] and self.config["extract_emails"]:
                # Use a separate browser instance for email extraction to isolate potential issues
                email_browser_id = None
                try:
                    # Check cache first
                    email_cache_key = f"email_{place_info['website']}"
                    cached_email = self.cache.get(email_cache_key)
                    if cached_email is not None: # Cache hit (could be "" if no email was found previously)
                        if cached_email:
                            place_info["email"] = cached_email
                            with self.lock: self.stats["email_found_count"] += 1
                            self.logger.info(f"Found cached email for {place_info['website']}: {cached_email}")
                        else:
                            self.logger.debug(f"Cached result indicates no email found for {place_info['website']}")
                    else: # Cache miss, perform live lookup
                        self.logger.debug(f"Performing live email lookup for {place_info['website']}")
                        email_browser_id = self.browser_pool.get_browser(timeout=self.config["email_timeout"]) # Use configured timeout
                        email_driver = self.browser_pool.get_driver(email_browser_id)
                        if email_driver:
                            email = self._extract_email_from_site(place_info["website"], email_driver)
                            self.cache.set(email_cache_key, email or "") # Cache the result (even if empty)
                            if email:
                                place_info["email"] = email
                                with self.lock: self.stats["email_found_count"] += 1
                        else:
                            self.logger.warning(f"Could not get browser for email extraction for {place_info['website']}")

                except TimeoutError:
                    self.logger.warning(f"Timeout getting browser for email extraction for {place_info['website']}")
                except Exception as email_err:
                    self.logger.warning(f"Email extraction failed for {place_info['website']}: {email_err}", exc_info=self.debug)
                    if email_browser_id is not None: self.browser_pool.report_error(email_browser_id, "EmailExtractError")
                finally:
                    if email_browser_id is not None: self.browser_pool.release_browser(email_browser_id)

            # --- Final Steps ---
            # Log success and update stats
            self.logger.info(f"Thread {thread_id} - Successfully extracted: {place_info['name']}")
            with self.lock:
                self.stats["successful_extractions"] += 1
                self.processed_links.add(url) # Add to processed only on success

            # Log business details to dedicated file
            business_log = {k: v for k, v in place_info.items() if k not in ['social_links', 'reviews']} # Exclude complex fields
            if place_info.get("social_links"):
                business_log.update({f"social_{k}": v for k, v in place_info["social_links"].items()}) # Flatten social links
            
            # Add review count rather than full review content
            if place_info.get("reviews"):
                business_log["review_snippets_count"] = len(place_info["reviews"])
            
            self.business_logger.info(json.dumps(business_log))

            return dict(place_info) # Convert back to regular dict

        except Exception as e:
            self.logger.error(f"Thread {thread_id} - Error extracting place info for {url[:80]}: {e}", exc_info=self.debug)
            with self.lock: self.stats["extraction_errors"] += 1
            # Don't add to processed_links on error
            return None


    def _selenium_fallback_extraction(self, driver, place_info):
        """Fallback extraction using Selenium when JavaScript extraction fails"""
        # Only try to extract values that weren't already found
        
        # Name (Crucial - try multiple selectors)
        if not place_info["name"]:
            name_selectors = [
                "h1", 
                "h1.DUwDvf", 
                "h1[class*='headline']", 
                "h1[class*='header']", 
                "[role='main'] h1", 
                "div[role='main'] div[tabindex='-1'] > div:first-child"
            ]
            for sel in name_selectors:
                try:
                    name_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for name_el in name_elements:
                        candidate_name = name_el.text.strip()
                        if candidate_name and len(candidate_name) > 1:
                            place_info["name"] = candidate_name
                            self.logger.debug(f"Extracted name via Selenium selector: {sel}")
                            break
                    if place_info["name"]:
                        break
                except Exception as e: 
                    self.logger.debug(f"Name selector {sel} error: {e}")

        # Address (Multiple selectors for different UI versions)
        if not place_info["address"]:
            address_selectors = [
                "button[data-item-id='address'] div.Io6YTe",
                "button[aria-label*='Address:']",
                "button[aria-label*='address']",
                "div[data-tooltip='Copy address']",
                "button[data-tooltip='Copy address']",
                "div.rogA2c div:nth-child(1)",  # First item in info section
                "div.LBgpqf div:nth-child(1)"   # Alternative info container
            ]
            
            for sel in address_selectors:
                try:
                    addr_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for addr_el in addr_elements:
                        # Try getting text content first
                        addr_text = addr_el.text.strip()
                        
                        # If no text, try aria-label attribute
                        if not addr_text and 'aria-label' in sel:
                            addr_text = addr_el.get_attribute('aria-label')
                            if addr_text:
                                addr_text = addr_text.replace("Address:", "").strip()
                        
                        if addr_text and len(addr_text) > 5:  # Basic validation
                            place_info["address"] = addr_text
                            self.logger.debug(f"Extracted address via selector: {sel}")
                            break
                    
                    if place_info["address"]:
                        break
                except Exception as e:
                    self.logger.debug(f"Address selector {sel} error: {e}")

        # Phone
        if not place_info["phone"]:
            phone_selectors = [
                "button[data-item-id^='phone:tel:'] div.Io6YTe",
                "button[aria-label*='Phone:']",
                "button[aria-label*='phone']",
                "div[data-tooltip='Copy phone number']",
                "button[data-tooltip='Copy phone number']",
                "div.rogA2c div:nth-child(2)",  # Often second item in info
                "div.LBgpqf div:nth-child(2)"   # Alternative container
            ]
            
            for sel in phone_selectors:
                try:
                    phone_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for phone_el in phone_elements:
                        # Try getting text content first
                        phone_text = phone_el.text.strip()
                        
                        # If no text, try aria-label attribute
                        if not phone_text and 'aria-label' in sel:
                            phone_text = phone_el.get_attribute('aria-label')
                            if phone_text:
                                phone_text = phone_text.replace("Phone:", "").strip()
                        
                        # Basic phone number validation
                        if phone_text and ('+' in phone_text or any(c.isdigit() for c in phone_text)):
                            place_info["phone"] = phone_text
                            self.logger.debug(f"Extracted phone via selector: {sel}")
                            break
                    
                    if place_info["phone"]:
                        break
                except Exception as e:
                    self.logger.debug(f"Phone selector {sel} error: {e}")

        # Website
        if not place_info["website"]:
            website_selectors = [
                "a[data-item-id='authority']",
                "a[aria-label*='Website:']",
                "a[data-tooltip='Open website']",
                "div.rogA2c a[target='_blank']",
                "div.LBgpqf a[target='_blank']"
            ]
            
            for sel in website_selectors:
                try:
                    web_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for web_el in web_elements:
                        website_url = web_el.get_attribute('href')
                        
                        # Skip Google-related links and maps links
                        if website_url and not website_url.startswith('https://www.google.com/') and '/maps/' not in website_url:
                            place_info["website"] = website_url
                            self.logger.debug(f"Extracted website via selector: {sel}")
                            break
                    
                    if place_info["website"]:
                        break
                except Exception as e:
                    self.logger.debug(f"Website selector {sel} error: {e}")

        # Category
        if not place_info["category"]:
            category_selectors = [
                "button[jsaction*='category']", 
                "div.fontBodyMedium > span > span > button",
                "span.YhemCb",  # Common category container
                "span.DkEaL",   # Alternative category container
                "div[role='main'] button.vwVdIc",
                "div.LBgpqf span.YhemCb"
            ]
            
            for sel in category_selectors:
                try:
                    cat_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for cat_el in cat_elements:
                        cat_text = cat_el.text.strip()
                        if cat_text and not cat_text.isdigit() and '$' not in cat_text:
                            place_info["category"] = cat_text
                            self.logger.debug(f"Extracted category via selector: {sel}")
                            break
                    
                    if place_info["category"]:
                        break
                except Exception as e:
                    self.logger.debug(f"Category selector {sel} error: {e}")

        # Rating & Reviews
        if not place_info["rating"] or not place_info["reviews_count"]:
            rating_selectors = [
                "div.F7nice", # Common rating container
                "span.fontDisplayLarge",  # Rating value
                "div.MyEned", # Alternative rating container
                "div.LBgpqf span.fontDisplayLarge",
                "div.rogA2c span.fontDisplayLarge",
                "div[role='main'] div.MyEned"
            ]
            
            for sel in rating_selectors:
                try:
                    rating_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for rating_el in rating_elements:
                        # Try to find rating value
                        if not place_info["rating"]:
                            rating_value_el = rating_el.find_element(By.CSS_SELECTOR, "span[aria-hidden='true']")
                            rating_text = rating_value_el.text.strip()
                            if rating_text and '.' in rating_text:
                                place_info["rating"] = rating_text
                        
                        # Try to find review count
                        if not place_info["reviews_count"]:
                            review_elements = rating_el.find_elements(By.CSS_SELECTOR, "span[aria-label*='reviews'], span[aria-label*='review']")
                            if review_elements:
                                review_text = review_elements[0].text.strip()
                                # Extract only digits
                                review_count_match = re.search(r'(\d{1,3}(?:,\d{3})*|\d+)', review_text)
                                if review_count_match:
                                    place_info["reviews_count"] = review_count_match.group(1).replace(',','')
                    
                    if place_info["rating"] and place_info["reviews_count"]:
                        self.logger.debug(f"Extracted rating and reviews via selector: {sel}")
                        break
                except Exception as e:
                    self.logger.debug(f"Rating/Review selector {sel} error: {e}")

        # Hours (if not already extracted)
        if not place_info["opening_hours"] and self.config.get("extract_hours", True):
            hours_selectors = [
                "div[data-attrid='kc:/location/location:hours'] div.webanswers-webanswers_table__webanswers-table", # Older format
                "div[jslog*='opening_hours'] div.MkV7Qc", # Hours container
                "div[jslog*='opening_hours'] table.WgFkxc", # Hours table
                "div[data-attrid='kc:/local:hours by day'] div.webanswers-webanswers_table__webanswers-table",
                "div[data-attrid='kc:/location/location:hours'] table",
                "div.LBgpqf div[jslog*='hours']",
                "div.rogA2c div[jslog*='hours']"
            ]
            
            for sel in hours_selectors:
                try:
                    hours_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for hours_el in hours_elements:
                        hours_text = hours_el.text.strip()
                        if hours_text and len(hours_text) > 10:
                            # Clean up hours text for consistent format
                            hours_text = hours_text.replace('\n', '; ').replace('day', 'day:')
                            place_info["opening_hours"] = hours_text
                            self.logger.debug(f"Extracted hours via selector: {sel}")
                            break
                    
                    if place_info["opening_hours"]:
                        break
                except Exception as e:
                    self.logger.debug(f"Hours selector {sel} error: {e}")
                    
            # Try clicking the hours button to expand if not found
            if not place_info["opening_hours"]:
                try:
                    hours_buttons = driver.find_elements(By.CSS_SELECTOR, "button[data-item-id='oh'], button[aria-label*='hour'], button[data-tooltip*='hour']")
                    for hours_btn in hours_buttons:
                        if hours_btn.is_displayed():
                            self.logger.debug("Clicking hours button to expand")
                            hours_btn.click()
                            time.sleep(1)  # Wait for expansion
                            
                            # Try to extract hours after expansion
                            for sel in hours_selectors:
                                try:
                                    hours_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                                    for hours_el in hours_elements:
                                        hours_text = hours_el.text.strip()
                                        if hours_text and len(hours_text) > 10:
                                            place_info["opening_hours"] = hours_text.replace('\n', '; ')
                                            break
                                    if place_info["opening_hours"]:
                                        break
                                except Exception:
                                    continue
                                    
                            # Try to close the expanded panel to avoid issues with future extractions
                            try:
                                close_buttons = driver.find_elements(By.CSS_SELECTOR, "button[aria-label='Close'], button.VfPpkd-icon-LgbsSe')
                                for close_btn in close_buttons:
                                    if close_btn.is_displayed():
                                        close_btn.click()
                                        break
                            except Exception:
                                pass
                                
                            if place_info["opening_hours"]:
                                break
                except Exception as click_err:
                    self.logger.debug(f"Error clicking hours button: {click_err}")

        # Extract reviews if enabled and not already found
        if not place_info["reviews"] and self.config.get("extract_reviews", True):
            try:
                # First check if review panel is already open
                review_selectors = [
                    "div.jANrlb div.wiI7pd",  # Review text containers
                    "div.SEmwNb div.wiI7pd",  # Alternative review container
                    "div[data-review-id] div.MyEned", # Review with ID
                    "div.rogA2c div.jftiEf", # Review section
                    "div.LBgpqf div.jftiEf"  # Alternative review section
                ]
                
                reviews_found = []
                for sel in review_selectors:
                    review_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for review_el in review_elements[:3]:  # Limit to first 3 reviews
                        review_text = review_el.text.strip()
                        if review_text and len(review_text) > 10:
                            reviews_found.append(review_text)
                
                # If no reviews found, try to click on reviews tab/link to load them
                if not reviews_found:
                    review_tab_selectors = [
                        "button[data-tab-index='1']",  # Common reviews tab
                        "a[href*='#reviews']",
                        "div[jslog*='reviews'] button",
                        "div.rogA2c button[jsaction*='reviews']",
                        "div.LBgpqf button[jsaction*='reviews']"
                    ]
                    
                    for tab_sel in review_tab_selectors:
                        try:
                            tab_elements = driver.find_elements(By.CSS_SELECTOR, tab_sel)
                            for tab_el in tab_elements:
                                if tab_el.is_displayed():
                                    self.logger.debug("Clicking reviews tab")
                                    tab_el.click()
                                    time.sleep(1.5)  # Wait for reviews to load
                                    
                                    # Now try to extract reviews again
                                    for sel in review_selectors:
                                        review_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                                        for review_el in review_elements[:3]:
                                            review_text = review_el.text.strip()
                                            if review_text and len(review_text) > 10:
                                                reviews_found.append(review_text)
                                    
                                    if reviews_found:
                                        break
                        except Exception:
                            continue
                        
                        if reviews_found:
                            break
                
                # Store found reviews
                if reviews_found:
                    place_info["reviews"] = reviews_found[:3]  # Limit to 3 reviews
                    self.logger.debug(f"Extracted {len(reviews_found)} reviews")
            except Exception as review_err:
                self.logger.debug(f"Error extracting reviews: {review_err}")

        # Extract price level if not already found
        if not place_info["price_level"]:
            try:
                price_selectors = [
                    "span.mgr77e", # Common price level indicator
                    "span.LMaH6e", # Alternative price container
                    "span[aria-label*='Price: ']",
                    "div.mgr77e"   # Another price container
                ]
                
                for sel in price_selectors:
                    price_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for price_el in price_elements:
                        price_text = price_el.text.strip()
                        if price_text and ('$' in price_text or '€' in price_text or '£' in price_text):
                            place_info["price_level"] = price_text
                            self.logger.debug(f"Extracted price level via selector: {sel}")
                            break
                    
                    if place_info["price_level"]:
                        break
            except Exception as price_err:
                self.logger.debug(f"Error extracting price level: {price_err}")

        # Extract business features/attributes if not already found
        if not place_info["features"]:
            try:
                feature_selectors = [
                    "div.RcCsl[role='region'] div.NGLtO", # Features section
                    "div.cW8rTb div.MGLtgb", # Service options
                    "div.aLfhNb div.dbeHre", # Amenities
                    "div.LBgpqf div.MGLtgb" # Alternative container
                ]
                
                features = []
                for sel in feature_selectors:
                    feature_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                    for feature_el in feature_elements:
                        feature_text = feature_el.text.strip()
                        if feature_text:
                            features.extend([f.strip() for f in feature_text.split('\n') if f.strip()])
                
                if features:
                    place_info["features"] = features
                    self.logger.debug(f"Extracted {len(features)} business features")
            except Exception as feature_err:
                self.logger.debug(f"Error extracting business features: {feature_err}")


    def _get_enhanced_js_extraction_script(self):
        """Returns the enhanced JavaScript code string for extracting business info with better coverage."""
        return """
            function extractBusinessInfo() {
                const data = {
                    name: "", 
                    address: "", 
                    phone: "", 
                    website: "", 
                    rating: "", 
                    reviews_count: "", 
                    category: "", 
                    opening_hours: "", 
                    price_level: "",
                    features: [],
                    reviews: []
                };

                // Enhanced text extraction function with multiple strategies
                const getText = (selectors, attribute = 'textContent') => {
                    if (typeof selectors === 'string') {
                        selectors = [selectors];
                    }
                    
                    for (const selector of selectors) {
                        try {
                            const elements = document.querySelectorAll(selector);
                            for (const el of elements) {
                                if (!el || !el.isConnected) continue;
                                
                                let value;
                                if (attribute === 'textContent' || attribute === 'innerText') {
                                    value = el[attribute];
                                } else {
                                    value = el.getAttribute(attribute);
                                }
                                
                                // Clean the text
                                if (value) {
                                    value = value.replace(/\\s+/g, ' ').trim();
                                    if (value.length > 0) return value;
                                }
                            }
                        } catch (e) {
                            console.warn(`Error with selector "${selector}":`, e);
                        }
                    }
                    return "";
                };

                // Find all elements that match a selector and extract their text
                const getAllText = (selectors, limit = 10) => {
                    if (typeof selectors === 'string') {
                        selectors = [selectors];
                    }
                    
                    const results = [];
                    for (const selector of selectors) {
                        try {
                            const elements = document.querySelectorAll(selector);
                            for (const el of elements) {
                                if (!el || !el.isConnected) continue;
                                
                                const text = el.textContent.replace(/\\s+/g, ' ').trim();
                                if (text && text.length > 0) {
                                    results.push(text);
                                    if (results.length >= limit) return results;
                                }
                            }
                        } catch (e) {
                            console.warn(`Error with getAllText "${selector}":`, e);
                        }
                    }
                    return results;
                };

                // Name - Try multiple selectors
                data.name = getText([
                    'h1', 
                    'h1.DUwDvf', 
                    'h1[class*="headline"]', 
                    'h1[class*="header"]', 
                    '[role="main"] h1'
                ]);

                // Address - Look for address elements in different UI variants
                data.address = getText([
                    'button[data-item-id="address"] div.Io6YTe',
                    'button[aria-label*="Address:"]',
                    'button[data-tooltip="Copy address"]',
                    'div[data-tooltip="Copy address"]',
                    'div.rogA2c div:nth-child(1)', // First entry in info section is often address
                    'div.eIuirG button div' // Another address container
                ]);
                
                // If address was found in aria-label, clean it
                if (data.address.toLowerCase().includes('address:')) {
                    data.address = data.address.replace(/address:/i, '').trim();
                }

                // Phone - Multiple selectors for phone extraction
                data.phone = getText([
                    'button[data-item-id^="phone:tel:"] div.Io6YTe',
                    'button[aria-label*="Phone:"]',
                    'button[data-tooltip="Copy phone number"]',
                    'div[data-tooltip="Copy phone number"]',
                    'div.rogA2c div:nth-child(2)' // Second entry often contains phone
                ]);
                
                // Clean phone number if found in aria-label
                if (data.phone.toLowerCase().includes('phone:')) {
                    data.phone = data.phone.replace(/phone:/i, '').trim();
                }

                // Website - Find website link
                data.website = getText([
                    'a[data-item-id="authority"]',
                    'a[aria-label*="Website:"]',
                    'a[data-tooltip="Open website"]',
                    'div.rogA2c a[target="_blank"]',
                    'div.LBgpqf a[target="_blank"]'
                ], 'href');
                
                // Filter out Google-related links that aren't actual business websites
                if (data.website && (data.website.includes('google.com/') || data.website.includes('/maps/'))) {
                    const nonGoogleLinks = document.querySelectorAll('a[href]:not([href*="google.com/"]):not([href*="/maps/"])');
                    for (const link of nonGoogleLinks) {
                        const href = link.getAttribute('href');
                        if (href && href.includes('http') && !href.includes('google.com/') && !href.includes('/maps/')) {
                            data.website = href;
                            break;
                        }
                    }
                }

                // Rating & Reviews - Find the rating container and extract data
                try {
                    // Try multiple selectors for rating
                    const ratingText = getText([
                        'div.F7nice span[aria-hidden="true"]',
                        'span.fontDisplayLarge[aria-hidden="true"]',
                        'div.MyEned span[aria-hidden="true"]'
                    ]);
                    
                    if (ratingText && ratingText.includes('.')) {
                        data.rating = ratingText;
                    }
                    
                    // Look for review count in multiple locations
                    const reviewText = getText([
                        'div.F7nice span[aria-label*="reviews"]',
                        'div.F7nice span[aria-label*="review"]',
                        'div.MyEned span[aria-label*="reviews"]',
                        'span.fontBodyMedium span[aria-label*="review"]'
                    ]);
                    
                    if (reviewText) {
                        const countMatch = reviewText.match(/(\\d{1,3}(?:[,.]\\d{3})*|\\d+)/);
                        if (countMatch) {
                            data.reviews_count = countMatch[0].replace(/[,.]/g, '');
                        }
                    }
                } catch (e) { 
                    console.warn("JS Error extracting rating/reviews:", e); 
                }

                // Category
                data.category = getText([
                    'button[jsaction*="category"]',
                    'div.fontBodyMedium > span > span > button',
                    'span.YhemCb', // Common category span
                    'span.DkEaL',  // Alternative category container
                    'div[role="main"] button.vwVdIc'
                ]);

                // Price Level - Look for price indicators
                try {
                    const priceText = getText([
                        'span.mgr77e', // Price level
                        'span.LMaH6e',
                        'span[aria-label*="Price: "]'
                    ]);
                    
                    if (priceText && (priceText.includes('$') || priceText.includes('€') || priceText.includes('£'))) {
                        data.price_level = priceText;
                    }
                } catch(e) { 
                    console.warn("JS Error extracting price level:", e); 
                }

                // Opening Hours
                try {
                    // First try to find hours directly
                    data.opening_hours = getText([
                        'div[data-attrid="kc:/location/location:hours"] div.webanswers-webanswers_table__webanswers-table',
                        'div[jslog*="opening_hours"] table.WgFkxc',
                        'div[jslog*="opening_hours"] div.MkV7Qc'
                    ]);
                    
                    // If not found, try extracting the text from the hours button
                    if (!data.opening_hours) {
                        data.opening_hours = getText([
                            'button[data-item-id="oh"] div.Io6YTe',
                            'button[aria-label*="hours"] div.Io6YTe',
                            'button[aria-label*="Open"] div.Io6YTe'
                        ]);
                    }
                    
                    // Clean up hours text
                    if (data.opening_hours) {
                        data.opening_hours = data.opening_hours.replace(/\\n/g, '; ').replace(/opening hours:/i, '').trim();
                    }
                } catch(e) { 
                    console.warn("JS Error extracting hours:", e); 
                }

                // Business Features/Attributes
                try {
                    const featureContainers = [
                        'div.RcCsl[role="region"] div.NGLtO', // Features section
                        'div.cW8rTb div.MGLtgb',             // Service options 
                        'div.aLfhNb div.dbeHre'              // Amenities
                    ];
                    
                    for (const selector of featureContainers) {
                        const elements = document.querySelectorAll(selector);
                        for (const el of elements) {
                            const featureText = el.textContent.trim();
                            if (featureText) {
                                featureText.split('\\n').forEach(feature => {
                                    const cleanFeature = feature.trim();
                                    if (cleanFeature && !data.features.includes(cleanFeature)) {
                                        data.features.push(cleanFeature);
                                    }
                                });
                            }
                        }
                    }
                } catch(e) {
                    console.warn("JS Error extracting business features:", e);
                }

                // Extract Reviews
                try {
                    const reviewContainers = [
                        'div.jANrlb div.wiI7pd',     // Review text containers
                        'div.SEmwNb div.wiI7pd',     // Alternative review container
                        'div[data-review-id] div.MyEned' // Review with ID
                    ];
                    
                    // Get up to 3 reviews
                    for (const selector of reviewContainers) {
                        const reviewElements = document.querySelectorAll(selector);
                        for (let i = 0; i < Math.min(reviewElements.length, 3); i++) {
                            const reviewText = reviewElements[i].textContent.trim();
                            if (reviewText && reviewText.length > 10 && !data.reviews.includes(reviewText)) {
                                data.reviews.push(reviewText);
                            }
                        }
                        
                        if (data.reviews.length >= 3) break;
                    }
                } catch(e) {
                    console.warn("JS Error extracting reviews:", e);
                }

                return data;
            }
            return extractBusinessInfo();
        """

    def extract_place_id(self, url):
        """Extract place ID (CID) from Google Maps URL using regex"""
        if not url: return ""
        try:
            # Pattern 1: Look for !1s([a-zA-Z0-9-_:]+) within the data=! part
            match1 = re.search(r'data=.*!1s([a-zA-Z0-9-_:]+)', url)
            if match1: return match1.group(1)

            # Pattern 2: Look for /place/[^/]+/data=!.*!1s([a-zA-Z0-9-_:]+)
            match2 = re.search(r'/place/[^/]+/data=.*!1s([a-zA-Z0-9-_:]+)', url)
            if match2: return match2.group(1)

            # Pattern 3: Look for query parameter place_id=
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            if 'place_id' in query_params: return query_params['place_id'][0]
            
            # Pattern 4: Extract from mid=([^&]+) parameter
            mid_match = re.search(r'mid=([^&]+)', url)
            if mid_match: return mid_match.group(1)

            # Pattern 5: Look for !1s([a-zA-Z0-9-_:]+) anywhere in the path/query (less specific)
            match4 = re.search(r'!1s([a-zA-Z0-9-_:]+)', url)
            if match4: return match4.group(1)

            self.logger.debug(f"Could not extract place ID from URL: {url}")
            return ""
        except Exception as e:
             self.logger.warning(f"Error parsing place ID from URL {url}: {e}")
             return ""


    def extract_coordinates_from_url(self, url):
        """Extract coordinates (lat, lng) from a Google Maps URL"""
        if not url: return ""
        try:
            # Try multiple regex patterns to handle different URL formats
            
            # Pattern 1: @lat,lng,zoom pattern
            coords_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+),(\d+\.?\d*)z', url)
            if coords_match:
                lat, lng = coords_match.group(1), coords_match.group(2)
                # Basic validation for latitude and longitude ranges
                if -90 <= float(lat) <= 90 and -180 <= float(lng) <= 180:
                    return f"{lat},{lng}"
            
            # Pattern 2: @lat,lng,data pattern
            coords_match2 = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)(?:,\d+\.?\d*)?[mz]/data', url)
            if coords_match2:
                lat, lng = coords_match2.group(1), coords_match2.group(2)
                if -90 <= float(lat) <= 90 and -180 <= float(lng) <= 180:
                    return f"{lat},{lng}"
                    
            # Pattern 3: ll=lat,lng pattern in query parameters
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            if 'll' in query_params:
                ll_parts = query_params['ll'][0].split(',')
                if len(ll_parts) >= 2:
                    lat, lng = ll_parts[0], ll_parts[1]
                    if -90 <= float(lat) <= 90 and -180 <= float(lng) <= 180:
                        return f"{lat},{lng}"
            
            # Try to find any coordinate-like pattern            
            coords_match3 = re.search(r'[-+]?\d+\.\d+,[-+]?\d+\.\d+', url)
            if coords_match3:
                coords = coords_match3.group(0).split(',')
                lat, lng = coords[0], coords[1]
                if -90 <= float(lat) <= 90 and -180 <= float(lng) <= 180:
                    return f"{lat},{lng}"
                    
            self.logger.debug(f"Could not extract coordinates from URL: {url}")
            return ""
        except Exception as e:
             self.logger.warning(f"Error parsing coordinates from URL {url}: {e}")
             return ""


    def extract_social_media_links(self, driver):
        """Extract social media links from a business page using enhanced JS"""
        self.logger.debug("Attempting to extract social media links...")
        try:
            social_links = driver.execute_script("""
                const socialLinks = {};
                
                // Extended list of social media domains for better coverage
                const socialDomains = {
                    'facebook.com': 'facebook', 
                    'fb.com': 'facebook', 
                    'fb.me': 'facebook',
                    'instagram.com': 'instagram',
                    'twitter.com': 'twitter', 
                    'x.com': 'twitter',
                    't.co': 'twitter',
                    'linkedin.com': 'linkedin',
                    'youtube.com': 'youtube',
                    'youtu.be': 'youtube',
                    'pinterest.com': 'pinterest',
                    'pin.it': 'pinterest',
                    'tiktok.com': 'tiktok',
                    'yelp.com': 'yelp',
                    'tripadvisor.com': 'tripadvisor',
                    'tripadvisor.co': 'tripadvisor',
                    'snapchat.com': 'snapchat',
                    'whatsapp.com': 'whatsapp',
                    'tumblr.com': 'tumblr',
                    'reddit.com': 'reddit',
                    'telegram.org': 'telegram',
                    't.me': 'telegram',
                    'threads.net': 'threads',
                    'medium.com': 'medium',
                    'vimeo.com': 'vimeo'
                };
                
                // Try multiple strategies to find social links
                
                // Strategy 1: Look for all links in the document
                const extractFromAllLinks = () => {
                    document.querySelectorAll('a[href]').forEach(link => {
                        const href = link.href;
                        if (!href || href.startsWith('mailto:') || href.startsWith('tel:')) return;
    
                        try {
                            const url = new URL(href);
                            const domain = url.hostname.replace(/^www\\./, '');
    
                            for (const [socialDomain, network] of Object.entries(socialDomains)) {
                                if (domain.includes(socialDomain)) {
                                    // Skip login/share/intent links
                                    if (href.includes('/sharer') || href.includes('/intent') || 
                                        href.includes('login') || href.includes('signup')) {
                                        continue;
                                    }
                                    
                                    // Prioritize profile-like links with longer paths
                                    if (!socialLinks[network] || 
                                        url.pathname.length > new URL(socialLinks[network]).pathname.length) {
                                        socialLinks[network] = href;
                                    }
                                }
                            }
                        } catch (e) { /* Ignore invalid URLs */ }
                    });
                };
                
                // Strategy 2: Look for links in business info area
                const extractFromBusinessInfo = () => {
                    const infoContainers = [
                        'div.rogA2c', 
                        'div.LBgpqf', 
                        'div[role="complementary"]'
                    ];
                    
                    for (const container of infoContainers) {
                        const containerEl = document.querySelector(container);
                        if (!containerEl) continue;
                        
                        containerEl.querySelectorAll('a[href]').forEach(link => {
                            const href = link.href;
                            if (!href || href.startsWith('mailto:') || href.startsWith('tel:')) return;
                            
                            try {
                                const url = new URL(href);
                                const domain = url.hostname.replace(/^www\\./, '');
                                
                                for (const [socialDomain, network] of Object.entries(socialDomains)) {
                                    if (domain.includes(socialDomain)) {
                                        // Skip problematic URLs
                                        if (href.includes('/sharer') || href.includes('/intent') || 
                                            href.includes('login') || href.includes('signup')) {
                                            continue;
                                        }
                                        
                                        // Store the social link
                                        socialLinks[network] = href;
                                    }
                                }
                            } catch (e) { /* Ignore invalid URLs */ }
                        });
                    }
                };
                
                // Strategy 3: Look for social media icons
                const extractFromIcons = () => {
                    const iconSelectors = [
                        'img[src*="facebook"], img[alt*="Facebook"]',
                        'img[src*="instagram"], img[alt*="Instagram"]',
                        'img[src*="twitter"], img[alt*="Twitter"], img[alt*="X"]',
                        'img[src*="linkedin"], img[alt*="LinkedIn"]',
                        'img[src*="youtube"], img[alt*="YouTube"]',
                        'img[src*="pinterest"], img[alt*="Pinterest"]',
                        'img[src*="tiktok"], img[alt*="TikTok"]'
                    ];
                    
                    for (const selector of iconSelectors) {
                        document.querySelectorAll(selector).forEach(img => {
                            // Find parent link
                            let parent = img.parentElement;
                            while (parent && parent.tagName !== 'A' && parent !== document.body) {
                                parent = parent.parentElement;
                            }
                            
                            if (parent && parent.tagName === 'A' && parent.href) {
                                try {
                                    const href = parent.href;
                                    const url = new URL(href);
                                    const domain = url.hostname.replace(/^www\\./, '');
                                    
                                    for (const [socialDomain, network] of Object.entries(socialDomains)) {
                                        if (domain.includes(socialDomain)) {
                                            socialLinks[network] = href;
                                        }
                                    }
                                } catch (e) { /* Ignore invalid URLs */ }
                            }
                        });
                    }
                };
                
                // Execute all strategies
                extractFromBusinessInfo();  // Start with most likely area
                extractFromIcons();         // Then look for icons
                extractFromAllLinks();      // Finally check all links
                
                // Cleanup for returned links
                for (const network in socialLinks) {
                    // Ensure URLs are clean - remove tracking parameters
                    try {
                        const url = new URL(socialLinks[network]);
                        // Remove common tracking parameters
                        ['utm_source', 'utm_medium', 'utm_campaign', 'fbclid', 'gclid'].forEach(param => {
                            url.searchParams.delete(param);
                        });
                        socialLinks[network] = url.toString();
                    } catch (e) { /* Keep original if cleaning fails */ }
                }
                
                return socialLinks;
            """)
            
            if social_links: 
                self.logger.debug(f"Found social links: {social_links}")
            return social_links if social_links else {}
        except Exception as e:
            self.logger.warning(f"Error extracting social media links via JS: {e}")
            
            # Fallback: Try Selenium approach if JS fails
            try:
                social_networks = {
                    'facebook': ['facebook.com', 'fb.com', 'fb.me'],
                    'instagram': ['instagram.com'],
                    'twitter': ['twitter.com', 'x.com', 't.co'],
                    'linkedin': ['linkedin.com'],
                    'youtube': ['youtube.com', 'youtu.be'],
                    'pinterest': ['pinterest.com', 'pin.it'],
                    'tiktok': ['tiktok.com']
                }
                
                selenium_social_links = {}
                
                # Get all links on the page
                links = driver.find_elements(By.TAG_NAME, 'a')
                for link in links:
                    try:
                        href = link.get_attribute('href')
                        if not href or href.startswith('mailto:') or href.startswith('tel:'):
                            continue
                            
                        # Check each social network domain
                        for network, domains in social_networks.items():
                            if any(domain in href for domain in domains):
                                # Skip login/share links
                                if any(x in href for x in ['/sharer', '/intent', 'login', 'signup']):
                                    continue
                                    
                                # Add or update the social link
                                if network not in selenium_social_links or len(selenium_social_links[network]) < len(href):
                                    selenium_social_links[network] = href
                    except Exception:
                        continue
                
                if selenium_social_links:
                    self.logger.debug(f"Found social links via Selenium fallback: {selenium_social_links}")
                return selenium_social_links
            except Exception as selenium_err:
                self.logger.warning(f"Selenium fallback for social links failed: {selenium_err}")
                return {}


    def _extract_email_from_site(self, website_url, driver):
        """Extract email using advanced techniques and heuristics from a website"""
        self.logger.info(f"Attempting email extraction from: {website_url}")
        try:
            # Set a reasonable page load timeout for the website
            driver.set_page_load_timeout(self.config["email_timeout"])
            
            # Try to load the website
            try:
                driver.get(website_url)
                
                # Add random delay to appear more human-like
                wait_time = random.uniform(2.0, 4.0) if self.config.get("randomize_delays", True) else 3.0
                time.sleep(wait_time)
                
            except TimeoutException:
                self.logger.warning(f"Timeout loading website: {website_url}")
                # Try to continue extraction even if page didn't fully load
            
            # Execute JS to find emails with improved regex and filtering
            emails = driver.execute_script("""
                // Comprehensive email regex
                const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
                
                // Get text content and HTML
                const pageText = document.body?.innerText || '';
                const pageHTML = document.documentElement?.outerHTML || '';
                let foundEmails = new Set();

                // Strategy 1: Find emails in visible text
                (pageText.match(emailRegex) || []).forEach(e => foundEmails.add(e.toLowerCase()));
                
                // Strategy 2: Find emails in source HTML (might catch obfuscated ones)
                (pageHTML.match(emailRegex) || []).forEach(e => foundEmails.add(e.toLowerCase()));

                // Strategy 3: Find emails in mailto links
                document.querySelectorAll('a[href^="mailto:"]').forEach(link => {
                    try {
                        const mailtoHref = link.getAttribute('href');
                        const emailPart = mailtoHref.substring(7).split('?')[0];
                        if (emailPart && emailPart.includes('@')) {
                            foundEmails.add(emailPart.toLowerCase());
                        }
                    } catch(e){}
                });
                
                // Strategy 4: Look for obfuscated emails with entities or script
                try {
                    // Look for elements with data attributes related to email
                    document.querySelectorAll('[data-email], [data-mail]').forEach(el => {
                        const emailData = el.getAttribute('data-email') || el.getAttribute('data-mail');
                        if (emailData && emailData.includes('@')) {
                            foundEmails.add(emailData.toLowerCase());
                        }
                    });
                    
                    // Check for contact forms that might have hidden email field
                    document.querySelectorAll('form[action*="contact"], form[id*="contact"], form[class*="contact"]').forEach(form => {
                        const hiddenFields = form.querySelectorAll('input[type="hidden"]');
                        hiddenFields.forEach(field => {
                            const value = field.value;
                            if (value && value.includes('@') && value.includes('.')) {
                                const matches = value.match(emailRegex);
                                if (matches) {
                                    matches.forEach(m => foundEmails.add(m.toLowerCase()));
                                }
                            }
                        });
                    });
                } catch(e) {}

                // Filter out common invalid/placeholder emails and image/font/css extensions
                const invalidPatterns = /example|placeholder|yourdomain|domain\\.com|sentry|wixpress|info@info|site@site|support@support|gmail@gmail|\\.(png|jpg|jpeg|gif|webp|svg|woff|woff2|ttf|css|js)$/i;
                const validEmails = Array.from(foundEmails).filter(email =>
                    !invalidPatterns.test(email) && 
                    email.includes('.') && 
                    email.length < 80 && // Basic length sanity check
                    email.split('@')[0].length >= 2 && // Local part must be at least 2 chars
                    email.split('@')[1].length >= 4    // Domain part must be at least 4 chars (a.bc)
                );

                // Prioritize common business emails
                const priorityPrefixes = [
                    'info@', 'contact@', 'hello@', 'support@', 'sales@', 'office@', 
                    'admin@', 'mail@', 'help@', 'booking@', 'reservations@', 'enquiry@', 'enquiries@'
                ];
                
                let primaryEmail = '';
                
                // First check domains matching the website
                try {
                    const currentDomain = window.location.hostname.replace('www.', '');
                    const domainEmails = validEmails.filter(email => email.split('@')[1].includes(currentDomain));
                    
                    if (domainEmails.length > 0) {
                        // Prioritize emails with domain matching the site
                        for (const prefix of priorityPrefixes) {
                            primaryEmail = domainEmails.find(e => e.startsWith(prefix));
                            if (primaryEmail) break;
                        }
                        
                        // If no priority match, take the first domain match
                        if (!primaryEmail) {
                            primaryEmail = domainEmails[0];
                        }
                    }
                } catch(e) {}
                
                // If no matching domain email, try priority prefixes
                if (!primaryEmail) {
                    for (const prefix of priorityPrefixes) {
                        primaryEmail = validEmails.find(e => e.startsWith(prefix));
                        if (primaryEmail) break;
                    }
                }

                // If still no primary email, return the first valid one
                return primaryEmail || (validEmails.length > 0 ? validEmails[0] : '');
            """)

            if emails:
                self.logger.info(f"Found email on {website_url}: {emails}")
                return emails
            
            # If JS approach didn't find email, try checking "Contact" page
            if not emails and self.config.get("deep_email_search", False):
                try:
                    # Find contact page link
                    contact_links = driver.find_elements(By.XPATH, "//a[contains(translate(text(), 'CONTACT', 'contact'), 'contact') or contains(@href, 'contact')]")
                    
                    if contact_links:
                        for link in contact_links[:2]:  # Try up to 2 contact links
                            try:
                                contact_url = link.get_attribute('href')
                                if contact_url and contact_url.startswith('http'):
                                    self.logger.debug(f"Checking contact page: {contact_url}")
                                    driver.get(contact_url)
                                    time.sleep(2)  # Wait for page to load
                                    
                                    # Try to extract email from contact page
                                    contact_email = driver.execute_script("""
                                        const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}/g;
                                        const pageText = document.body.innerText || '';
                                        const pageHTML = document.documentElement.outerHTML || '';
                                        let foundEmails = new Set();
                                        
                                        // Check visible text
                                        (pageText.match(emailRegex) || []).forEach(e => foundEmails.add(e.toLowerCase()));
                                        
                                        // Check HTML
                                        (pageHTML.match(emailRegex) || []).forEach(e => foundEmails.add(e.toLowerCase()));
                                        
                                        // Check mailto links (often on contact pages)
                                        document.querySelectorAll('a[href^="mailto:"]').forEach(link => {
                                            const mailtoHref = link.getAttribute('href');
                                            const emailPart = mailtoHref.substring(7).split('?')[0];
                                            if (emailPart && emailPart.includes('@')) {
                                                foundEmails.add(emailPart.toLowerCase());
                                            }
                                        });
                                        
                                        // Filter and prioritize
                                        const invalidPatterns = /example|placeholder|yourdomain|domain\\.com|sentry|wixpress|\\.(png|jpg|jpeg|gif|webp|svg|woff|woff2|ttf|css)$/i;
                                        const validEmails = Array.from(foundEmails).filter(email =>
                                            !invalidPatterns.test(email) && email.includes('.') && email.length < 80
                                        );
                                        
                                        // Prioritize domain-matching emails
                                        try {
                                            const currentDomain = window.location.hostname.replace('www.', '');
                                            const domainEmail = validEmails.find(email => email.split('@')[1].includes(currentDomain));
                                            if (domainEmail) return domainEmail;
                                        } catch(e) {}
                                        
                                        // Return first valid email or empty string
                                        return validEmails.length > 0 ? validEmails[0] : '';
                                    """)
                                    
                                    if contact_email:
                                        self.logger.info(f"Found email on contact page: {contact_email}")
                                        return contact_email
                            except Exception as contact_err:
                                self.logger.debug(f"Error checking contact page: {contact_err}")
                except Exception as deep_search_err:
                    self.logger.debug(f"Error during deep email search: {deep_search_err}")
            
            self.logger.info(f"No valid email found on {website_url}")
            return ""

        except TimeoutException:
            self.logger.warning(f"Timeout loading website for email extraction: {website_url}")
            return ""
        except WebDriverException as wde:
             # Handle specific WebDriver errors like certificate issues, connection refused
             if "ERR_CONNECTION_REFUSED" in str(wde) or "ERR_NAME_NOT_RESOLVED" in str(wde):
                 self.logger.warning(f"Could not connect to website {website_url}: {str(wde).splitlines()[0]}")
             elif "ERR_CERT_AUTHORITY_INVALID" in str(wde) or "ERR_SSL_PROTOCOL_ERROR" in str(wde):
                 self.logger.warning(f"SSL/Certificate error accessing {website_url}. Skipping email check.")
             else:
                 self.logger.warning(f"WebDriver error during email extraction from {website_url}: {type(wde).__name__} - {str(wde).splitlines()[0]}")
             return ""
        except Exception as e:
            self.logger.warning(f"Generic error during email extraction from {website_url}: {type(e).__name__} - {e}")
            return ""
        finally:
            # Reset page load timeout to default if changed
             try:
                 driver.set_page_load_timeout(60) # Reset to default used elsewhere
             except Exception: pass # Ignore errors if driver is already closed
        # Note: Browser release is handled by the caller (extract_place_info)


    # --- Main Scraping Logic ---
    def scrape(self, query, location, grid_size_meters=250, max_results=None):
        """Main method to scrape businesses using the enhanced grid approach with parallelism"""
        self.stats["start_time"] = datetime.now()
        start_time = self.stats["start_time"]
        self.config["max_results"] = max_results # Store for access in threads if needed
        self.config["grid_size_meters"] = grid_size_meters # Store grid size

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
            if not bounds:
                 self.logger.error("Failed to get city boundaries. Aborting.")
                 return []

            grid = self.create_optimal_grid(bounds, grid_size_meters)
            if not grid:
                 self.logger.error("Failed to create grid. Aborting.")
                 return []

            # Sort grid cells by density (center first)
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
        """Search, extract links, and process businesses for a single grid cell. Returns"""

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
                if detail_browser_id is not None: self.browser_pool.report_error(detail_browser_id, "DetailExtractError")
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
        print(f"Query: '{query}'")
        print(f"Max results: {max_results or 'unlimited'}")
        print(f"Max workers: {self.max_workers}")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
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
        """Save results data to CSV file with comprehensive field handling"""
        if not data: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Dynamically determine headers based on all keys present in the data
            all_keys = set()
            social_keys = set()
            feature_keys = set()
            
            for row in data:
                 all_keys.update(k for k in row.keys() if k not in ['social_links', 'reviews', 'features'])
                 if isinstance(row.get("social_links"), dict):
                     social_keys.update(f"social_{net}" for net in row["social_links"])
                 if isinstance(row.get("features"), list):
                     feature_keys.add("features")

            # Define preferred order of columns
            preferred_order = [
                "name", "category", "address", "coordinates", "phone", 
                "email", "website", "maps_url", "rating", "reviews_count",
                "price_level", "opening_hours", "place_id", "grid_cell", "scrape_timestamp"
            ]

            # Arrange fields in proper order
            fieldnames = [f for f in preferred_order if f in all_keys]
            remaining_keys = sorted(list(all_keys - set(preferred_order)))
            
            # Add feature fields if present
            if feature_keys:
                remaining_keys.append("features")
                
            # Add social media fields
            if social_keys:
                social_fields = sorted(list(social_keys))
                remaining_keys.extend(social_fields)
                
            # Combine all fields
            fieldnames.extend(remaining_keys)

            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                for result in data:
                    row_data = result.copy()
                    
                    # Handle special fields
                    
                    # Flatten social links
                    if isinstance(row_data.get("social_links"), dict):
                        for network, url in row_data["social_links"].items():
                            row_data[f"social_{network}"] = url
                    if "social_links" in row_data: 
                        del row_data["social_links"]
                        
                    # Convert features list to string
                    if isinstance(row_data.get("features"), list):
                        row_data["features"] = "; ".join(row_data["features"])
                    
                    # Don't include full review content in CSV - just count them
                    if "reviews" in row_data:
                        row_data["review_count"] = len(row_data["reviews"]) if isinstance(row_data["reviews"], list) else 0
                        del row_data["reviews"]
                    
                    writer.writerow(row_data)
            
            self.logger.debug(f"CSV saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving CSV to {filename}: {e}", exc_info=True)


    def save_to_json(self, filename, data):
        """Save results data to JSON file with pretty formatting"""
        if not data: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as jsonfile:
                json.dump(data, jsonfile, indent=2, ensure_ascii=False) # Use indent=2 for readability
            self.logger.debug(f"JSON saved to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving JSON to {filename}: {e}", exc_info=True)


    def save_to_excel(self, filename, data):
        """Save results data to Excel file with advanced formatting"""
        if not data or not PANDAS_AVAILABLE: return
        try:
            filepath = Path(filename)
            filepath.parent.mkdir(parents=True, exist_ok=True)

            # Prepare data for DataFrame, handling complex fields
            df_data = []
            all_social_networks = set()
            
            for result in data:
                 row = result.copy()
                 
                 # Handle social links (flatten)
                 socials = row.pop("social_links", {})
                 if isinstance(socials, dict):
                     for network, url in socials.items():
                         col_name = f"social_{network}"
                         row[col_name] = url
                         all_social_networks.add(col_name)
                 
                 # Convert features list to string
                 if isinstance(row.get("features"), list):
                     row["features"] = "; ".join(row["features"])
                 
                 # Convert reviews to count and sample
                 if "reviews" in row:
                     reviews = row.pop("reviews", [])
                     row["review_count"] = len(reviews) if isinstance(reviews, list) else 0
                     if isinstance(reviews, list) and reviews:
                         # Store first review as sample
                         row["review_sample"] = reviews[0][:200] + "..." if len(reviews[0]) > 200 else reviews[0]
                 
                 df_data.append(row)

            # Create DataFrame
            df = pd.DataFrame(df_data)

            # Define column order
            preferred_order = [
                "name", "category", "address", "coordinates", "phone",
                "email", "website", "maps_url", "rating", "reviews_count",
                "review_count", "review_sample", "price_level", "opening_hours", 
                "features", "place_id", "grid_cell", "scrape_timestamp"
            ]
            social_cols = sorted(list(all_social_networks))
            
            # Create final column order
            final_order = [col for col in preferred_order if col in df.columns]
            final_order.extend(social_cols)
            other_cols = sorted([col for col in df.columns if col not in final_order and col not in social_cols])
            final_order.extend(other_cols)

            # Reorder DataFrame columns (keeping only those that exist)
            existing_cols = [col for col in final_order if col in df.columns]
            df = df[existing_cols]

            # Create a styled Excel file with formatting
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Businesses')
                
                # Get the workbook and worksheet
                workbook = writer.book
                worksheet = writer.sheets['Businesses']
                
                # Apply formatting
                # Freeze the header row
                worksheet.freeze_panes = 'A2'
                
                # Auto-adjust column widths
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if cell.value:
                                cell_length = len(str(cell.value))
                                if cell_length > max_length:
                                    max_length = cell_length
                        except:
                            pass
                    # Set width with some padding
                    adjusted_width = max(max_length + 2, 10)
                    # Cap width to prevent extremely wide columns
                    worksheet.column_dimensions[column_letter].width = min(adjusted_width, 50)
            
            self.logger.info(f"Saved Excel version to {filepath}")
        except Exception as e:
            self.logger.error(f"Error saving Excel to {filename}: {e}", exc_info=True)


    def generate_statistics_report(self):
        """Generate a comprehensive report with statistics about the scraped data"""
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
            report["price_levels"] = Counter()
            report["features"] = Counter()
            
            # For numerical analysis
            ratings = []
            reviews = []
            email_domains = Counter()
            websites_by_tld = Counter()

            for result in results_copy:
                # Count basic fields
                if result.get("category"): report["categories"][result["category"]] += 1
                if result.get("email"): 
                    report["with_email"] += 1
                    # Extract email domain
                    if '@' in result["email"]:
                        domain = result["email"].split('@')[-1].lower()
                        email_domains[domain] += 1
                if result.get("website"): 
                    report["with_website"] += 1
                    # Extract TLD
                    try:
                        parsed = urlparse(result["website"])
                        if parsed.netloc:
                            tld = parsed.netloc.split('.')[-1].lower()
                            websites_by_tld[tld] += 1
                    except:
                        pass
                if result.get("phone"): report["with_phone"] += 1
                if result.get("grid_cell"): report["businesses_by_grid_cell"][result["grid_cell"]] += 1
                if result.get("opening_hours"): report["with_hours"] += 1
                if result.get("price_level"): report["price_levels"][result["price_level"]] += 1
                
                # Track features
                if isinstance(result.get("features"), list):
                    for feature in result["features"]:
                        report["features"][feature] += 1
                
                # Social networks
                if isinstance(result.get("social_links"), dict):
                    for network in result["social_links"]:
                        report[f"with_social_{network}"] += 1
                
                # Numerical data for statistics
                if result.get("rating"):
                    try: 
                        ratings.append(float(str(result["rating"]).replace(',', '.')))
                    except (ValueError, TypeError): 
                        pass
                if result.get("reviews_count"):
                    try: 
                        reviews.append(int(str(result["reviews_count"]).replace(',', '').replace(' ', '')))
                    except (ValueError, TypeError): 
                        pass

            # Calculate statistics for numerical fields
            report["with_rating"] = len(ratings)
            if ratings:
                report["avg_rating"] = round(statistics.mean(ratings), 2)
                report["median_rating"] = round(statistics.median(ratings), 1)
                report["min_rating"] = min(ratings)
                report["max_rating"] = max(ratings)
            
            report["total_reviews"] = sum(reviews)
            if reviews:
                report["avg_reviews"] = round(statistics.mean(reviews), 1)
                report["median_reviews"] = int(statistics.median(reviews))
                report["min_reviews"] = min(reviews)
                report["max_reviews"] = max(reviews)

            # Top categories and features (limited to top 20)
            report["top_categories"] = {cat: count for cat, count in report["categories"].most_common(20)}
            report["top_features"] = {feature: count for feature, count in report["features"].most_common(20)}
            report["top_email_domains"] = {domain: count for domain, count in email_domains.most_common(10)}
            report["top_website_tlds"] = {tld: count for tld, count in websites_by_tld.most_common(10)}

            # Calculate percentages
            total = report["total_businesses"]
            if total > 0:
                 report["email_percentage"] = round((report["with_email"] / total) * 100, 1)
                 report["website_percentage"] = round((report["with_website"] / total) * 100, 1)
                 report["phone_percentage"] = round((report["with_phone"] / total) * 100, 1)
                 report["rating_percentage"] = round((report["with_rating"] / total) * 100, 1)
                 report["hours_percentage"] = round((report["with_hours"] / total) * 100, 1)
                 # Calculate social media percentages
                 for key in list(report.keys()):
                     if key.startswith("with_social_"):
                         network = key.replace("with_social_", "")
                         report[f"{network}_percentage"] = round((report[key] / total) * 100, 1)

            # Add scraping duration
            if self.stats["start_time"]:
                elapsed_seconds = (datetime.now() - self.stats["start_time"]).total_seconds()
                report["scrape_duration_minutes"] = round(elapsed_seconds / 60, 2)
                report["scrape_duration_formatted"] = self.get_elapsed_time()

            # Add scraping stats
            report["scrape_stats"] = {
                 "total_grid_cells": self.stats["grid_cells_total"],
                 "processed_grid_cells": self.stats["grid_cells_processed"],
                 "empty_grid_cells": self.stats["grid_cells_empty"],
                 "consent_pages_handled": self.stats["consent_pages_handled"],
                 "extraction_errors": self.stats["extraction_errors"],
                 "rate_limit_hits": self.stats["rate_limit_hits"],
                 "session_id": self.session_id,
                 "businesses_per_hour": round((total / max(elapsed_seconds, 1)) * 3600, 1) if self.stats["start_time"] else 0
            }

            # Save JSON report
            report_filename = self.reports_dir / f"statistics_report_{self.session_id}.json"
            with open(report_filename, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str) # Use default=str for Counter objects
            self.logger.info(f"Statistics report saved to {report_filename}")

            # Generate HTML report with visualizations
            if MATPLOTLIB_AVAILABLE and not self.no_images:
                self.generate_html_report(report)
            else:
                self.generate_simple_html_report(report)

            return report
        except Exception as e:
            self.logger.error(f"Error generating statistics report: {e}", exc_info=True)
            return None


    def generate_html_report(self, stats):
        """Generate HTML report with visualizations using Matplotlib"""
        try:
            # Initialize chart paths
            category_chart_path = None
            info_chart_path = None
            rating_chart_path = None
            feature_chart_path = None

            # --- Generate charts ---
            if MATPLOTLIB_AVAILABLE and not self.no_images:
                # 1. Create category chart
                if stats.get("top_categories"):
                    try:
                        fig, ax = plt.subplots(figsize=(12, 7))
                        categories = list(stats["top_categories"].keys())
                        counts = list(stats["top_categories"].values())
                        
                        # Limit to top 15 for readability
                        if len(categories) > 15:
                            categories = categories[:15]
                            counts = counts[:15]
                        
                        # Create horizontal bar chart for better label readability
                        y_pos = np.arange(len(categories))
                        ax.barh(y_pos, counts, align='center', color='skyblue')
                        ax.set_yticks(y_pos)
                        ax.set_yticklabels(categories)
                        ax.invert_yaxis()  # labels read top-to-bottom
                        ax.set_xlabel('Number of Businesses')
                        ax.set_title('Top Business Categories Found')
                        
                        # Add counts at the end of the bars
                        for i, v in enumerate(counts):
                            ax.text(v + 1, i, str(v), color='blue', va='center', fontweight='bold', fontsize=9)

                        plt.tight_layout()
                        category_chart_path_obj = self.reports_dir / f"category_chart_{self.session_id}.png"
                        plt.savefig(category_chart_path_obj)
                        category_chart_path = category_chart_path_obj.name # Use relative name for HTML
                        plt.close(fig)
                        self.logger.info(f"Category chart saved to {category_chart_path_obj}")
                    except Exception as chart_err:
                        self.logger.error(f"Failed to generate category chart: {chart_err}")

                # 2. Create information availability chart (pie chart)
                try:
                    fig, ax = plt.subplots(figsize=(8, 5))
                    info_labels = ['With Email', 'With Website', 'With Phone', 'With Rating', 'With Hours']
                    info_counts = [
                        stats.get("with_email", 0), 
                        stats.get("with_website", 0),
                        stats.get("with_phone", 0), 
                        stats.get("with_rating", 0),
                        stats.get("with_hours", 0)
                    ]
                    total_biz = stats.get("total_businesses", 1) # Avoid division by zero
                    info_pcts = [(c / max(1,total_biz)) * 100 for c in info_counts] # Ensure total_biz >= 1

                    # Use a pie chart for percentages
                    labels_pct = [f'{label}\n({pct:.1f}%)' for label, pct in zip(info_labels, info_pcts)]
                    ax.pie(info_counts, labels=labels_pct, autopct='%1.1f%%', startangle=90, 
                           colors=['#ff9999','#66b3ff','#99ff99','#ffcc99', '#c2c2f0'])
                    ax.axis('equal') # Equal aspect ratio ensures that pie is drawn as a circle.
                    plt.title('Percentage of Businesses with Key Information')

                    plt.tight_layout()
                    info_chart_path_obj = self.reports_dir / f"info_chart_{self.session_id}.png"
                    plt.savefig(info_chart_path_obj)
                    info_chart_path = info_chart_path_obj.name # Use relative name
                    plt.close(fig)
                    self.logger.info(f"Info chart saved to {info_chart_path_obj}")
                except Exception as chart_err:
                    self.logger.error(f"Failed to generate info chart: {chart_err}")
                
                # 3. Create rating distribution chart (histogram)
                if stats.get("with_rating", 0) > 0:
                    try:
                        fig, ax = plt.subplots(figsize=(8, 5))
                        # Extract ratings from results
                        ratings = []
                        for result in self.results:
                            if result.get("rating"):
                                try:
                                    ratings.append(float(str(result["rating"]).replace(',', '.')))
                                except (ValueError, TypeError):
                                    pass
                        
                        if ratings:
                            # Create histogram
                            ax.hist(ratings, bins=10, color='lightgreen', edgecolor='black', alpha=0.7)
                            ax.set_xlabel('Rating')
                            ax.set_ylabel('Number of Businesses')
                            ax.set_title('Distribution of Business Ratings')
                            ax.grid(axis='y', alpha=0.75)
                            
                            # Add average rating line
                            avg_rating = statistics.mean(ratings)
                            ax.axvline(avg_rating, color='red', linestyle='dashed', linewidth=1)
                            ax.text(avg_rating, ax.get_ylim()[1]*0.9, f'Avg: {avg_rating:.2f}', 
                                    color='red', fontweight='bold')
                            
                            plt.tight_layout()
                            rating_chart_path_obj = self.reports_dir / f"rating_chart_{self.session_id}.png"
                            plt.savefig(rating_chart_path_obj)
                            rating_chart_path = rating_chart_path_obj.name
                            plt.close(fig)
                            self.logger.info(f"Rating chart saved to {rating_chart_path_obj}")
                    except Exception as chart_err:
                        self.logger.error(f"Failed to generate rating chart: {chart_err}")
                
                # 4. Create features chart (top 10)
                if stats.get("top_features"):
                    try:
                        fig, ax = plt.subplots(figsize=(10, 6))
                        features = list(stats["top_features"].keys())[:10]  # Top 10 features
                        counts = list(stats["top_features"].values())[:10]
                        
                        # Horizontal bar chart
                        y_pos = np.arange(len(features))
                        ax.barh(y_pos, counts, align='center', color='lightblue')
                        ax.set_yticks(y_pos)
                        ax.set_yticklabels(features)
                        ax.invert_yaxis()
                        ax.set_xlabel('Number of Businesses')
                        ax.set_title('Top 10 Business Features/Attributes')
                        
                        # Add count labels
                        for i, v in enumerate(counts):
                            ax.text(v + 1, i, str(v), color='blue', va='center', fontsize=9)
                        
                        plt.tight_layout()
                        feature_chart_path_obj = self.reports_dir / f"feature_chart_{self.session_id}.png"
                        plt.savefig(feature_chart_path_obj)
                        feature_chart_path = feature_chart_path_obj.name
                        plt.close(fig)
                        self.logger.info(f"Feature chart saved to {feature_chart_path_obj}")
                    except Exception as chart_err:
                        self.logger.error(f"Failed to generate feature chart: {chart_err}")

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
                    .two-col {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
                    @media (max-width: 768px) {{ .two-col {{ grid-template-columns: 1fr; }} }}
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
                         {f'<div class="chart"><img src="{rating_chart_path}" alt="Rating Distribution Chart"></div>' if rating_chart_path else ""}
                    </div>

                    <div class="two-col">
                        <div class="section">
                            <h2>Business Categories</h2>
                            {'<div class="chart"><img src="' + category_chart_path + '" alt="Business Categories Chart"></div>' if category_chart_path else "<p>Category chart could not be generated.</p>"}
                            {'<table><thead><tr><th>Category</th><th>Count</th></tr></thead><tbody>' + ''.join([f'<tr><td>{cat}</td><td>{count}</td></tr>' for cat, count in stats.get("top_categories", {}).items()]) + '</tbody></table>' if stats.get("top_categories") else ""}
                        </div>

                        <div class="section">
                            <h2>Business Features</h2>
                            {'<div class="chart"><img src="' + feature_chart_path + '" alt="Business Features Chart"></div>' if feature_chart_path else "<p>Features chart could not be generated.</p>"}
                            {'<table><thead><tr><th>Feature</th><th>Count</th></tr></thead><tbody>' + ''.join([f'<tr><td>{feature}</td><td>{count}</td></tr>' for feature, count in stats.get("top_features", {}).items()]) + '</tbody></table>' if stats.get("top_features") else ""}
                        </div>
                    </div>

                    <div class="section">
                        <h2>Information Availability</h2>
                        {'<div class="chart"><img src="' + info_chart_path + '" alt="Information Availability Chart"></div>' if info_chart_path else "<p>Info availability chart could not be generated.</p>"}
                        
                        <div class="two-col">
                            <div>
                                <h3>Top Email Domains</h3>
                                {'<table><thead><tr><th>Domain</th><th>Count</th></tr></thead><tbody>' + ''.join([f'<tr><td>{domain}</td><td>{count}</td></tr>' for domain, count in stats.get("top_email_domains", {}).items()]) + '</tbody></table>' if stats.get("top_email_domains") else "<p>No email domain data available.</p>"}
                            </div>
                            <div>
                                <h3>Website TLDs</h3>
                                {'<table><thead><tr><th>TLD</th><th>Count</th></tr></thead><tbody>' + ''.join([f'<tr><td>.{tld}</td><td>{count}</td></tr>' for tld, count in stats.get("top_website_tlds", {}).items()]) + '</tbody></table>' if stats.get("top_website_tlds") else "<p>No website TLD data available.</p>"}
                            </div>
                        </div>
                    </div>

                    <div class="section">
                        <h2>Social Media Presence</h2>
                        <div class="stats-grid">
                            {''.join([f'<div class="stat-card"><div class="stat-number">{stats.get(f"with_social_{network}", 0)}</div><div class="stat-label">{network.capitalize()} ({stats.get(f"{network}_percentage", 0):.1f}%)</div></div>' for network in ["facebook", "instagram", "twitter", "linkedin", "youtube"] if stats.get(f"with_social_{network}", 0) > 0])}
                        </div>
                    </div>

                    <div class="section">
                        <h2>Scraping Performance</h2>
                        <div class="stats-grid">
                            <div class="stat-card"><div class="stat-number">{stats.get("scrape_duration_minutes", 0):.1f}</div><div class="stat-label">Minutes</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("scrape_stats", {}).get("businesses_per_hour", 0):.1f}</div><div class="stat-label">Businesses/Hour</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("scrape_stats", {}).get("total_grid_cells", 0)}</div><div class="stat-label">Grid Cells</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("scrape_stats", {}).get("processed_grid_cells", 0)}</div><div class="stat-label">Processed Cells</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("scrape_stats", {}).get("empty_grid_cells", 0)}</div><div class="stat-label">Empty Cells</div></div>
                        </div>
                        <div style="margin-top: 20px;">
                            <h3>Additional Statistics</h3>
                            <ul>
                                <li>Consent Pages Handled: {stats.get("scrape_stats", {}).get("consent_pages_handled", 0)}</li>
                                <li>Extraction Errors / Skips: {stats.get("scrape_stats", {}).get("extraction_errors", 0)}</li>
                                <li>Potential Rate Limit Hits: {stats.get("scrape_stats", {}).get("rate_limit_hits", 0)}</li>
                                <li>Max Workers Used: {self.max_workers}</li>
                                <li>Total Runtime: {stats.get("scrape_duration_formatted", "00:00:00")}</li>
                            </ul>
                        </div>
                    </div>

                    <div class="footer">
                        <p>Generated by Google Maps Grid Scraper v{VERSION}</p>
                        <p>Results saved to: {self.results_dir}/google_maps_data_{self.session_id}.csv/.json/.xlsx</p>
                    </div>
                </div>
            </body>
            </html>
            """

            # Save HTML report
            html_report_path = self.reports_dir / f"report_{self.session_id}.html"
            with open(html_report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"HTML report saved to {html_report_path}")

        except Exception as e:
            self.logger.error(f"Error generating HTML report: {e}", exc_info=True)


    def generate_simple_html_report(self, stats):
        """Generate a simpler HTML report without visualizations when Matplotlib is not available"""
        try:
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
                    .stat-card {{ background-color: #e8f0fe; border-radius: 5px; padding: 15px; text-align: center; }}
                    .stat-number {{ font-size: 2.2em; font-weight: bold; color: #1a73e8; margin-bottom: 5px; }}
                    .stat-label {{ font-size: 0.9em; color: #5f6368; }}
                    .section {{ background-color: #fff; border: 1px solid #e0e0e0; border-radius: 5px; padding: 20px; margin-bottom: 30px; }}
                    .section h2 {{ margin-top: 0; color: #4285F4; border-bottom: 2px solid #e0e0e0; padding-bottom: 10px; margin-bottom: 20px; }}
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
                            <div class="stat-card"><div class="stat-number">{stats.get("total_businesses", 0)}</div><div class="stat-label">Total Businesses</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("unique_businesses", 0)}</div><div class="stat-label">Unique Businesses</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("email_percentage", 0):.1f}%</div><div class="stat-label">With Email</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("website_percentage", 0):.1f}%</div><div class="stat-label">With Website</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("phone_percentage", 0):.1f}%</div><div class="stat-label">With Phone</div></div>
                            <div class="stat-card"><div class="stat-number">{stats.get("rating_percentage", 0):.1f}%</div><div class="stat-label">With Rating</div></div>
                        </div>
                    </div>

                    <div class="section">
                        <h2>Ratings & Reviews</h2>
                        <ul>
                            <li>Average Rating: {stats.get("avg_rating", 0):.2f}</li>
                            <li>Median Rating: {stats.get("median_rating", 0):.1f}</li>
                            <li>Total Reviews: {stats.get("total_reviews", 0):,}</li>
                            <li>Average Reviews per Business: {stats.get("avg_reviews", 0):.1f}</li>
                            <li>Median Reviews: {stats.get("median_reviews", 0):,}</li>
                        </ul>
                    </div>

                    <div class="section">
                        <h2>Top Business Categories</h2>
                        <table>
                            <thead><tr><th>Category</th><th>Count</th></tr></thead>
                            <tbody>
                                {''.join([f'<tr><td>{cat}</td><td>{count}</td></tr>' for cat, count in stats.get("top_categories", {}).items()][:15])}
                            </tbody>
                        </table>
                    </div>

                    <div class="section">
                        <h2>Scraping Performance</h2>
                        <ul>
                            <li>Total Runtime: {stats.get("scrape_duration_minutes", 0):.2f} minutes</li>
                            <li>Businesses per Hour: {stats.get("scrape_stats", {}).get("businesses_per_hour", 0):.1f}</li>
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
                        <p>Results saved to: {self.results_dir}/google_maps_data_{self.session_id}.csv/.json/.xlsx</p>
                    </div>
                </div>
            </body>
            </html>
            """

            html_report_path = self.reports_dir / f"report_{self.session_id}.html"
            with open(html_report_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            self.logger.info(f"Simple HTML report saved to {html_report_path}")
        except Exception as e:
            self.logger.error(f"Error generating simple HTML report: {e}", exc_info=True)


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

    # New advanced options
    parser.add_argument('--driver-path', type=str, help='Path to chromedriver executable')
    parser.add_argument('--chrome-binary', type=str, help='Path to Chrome binary')
    parser.add_argument('--user-data-dir', type=str, help='Directory for browser profiles')
    parser.add_argument('--deep-email', action='store_true', help='Enable deep email search (follows contact page links)')
    parser.add_argument('--no-reviews', dest='extract_reviews', action='store_false', default=True, help='Skip extracting review snippets')
    
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
            browser_error_threshold=args.retries, # Use retries for browser health threshold
            no_images=args.no_images,
            proxy_list=proxy_list,
            user_data_dir=args.user_data_dir,
            driver_path=args.driver_path,
            chrome_binary=args.chrome_binary
        )

        # Set configuration options
        scraper.config["extract_emails"] = args.extract_emails
        scraper.config["grid_size_meters"] = args.grid_size # Store grid size
        scraper.config["deep_email_search"] = args.deep_email
        scraper.config["extract_reviews"] = args.extract_reviews

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
            session_csv_path = results_dir / f"google_maps_data_{scraper.session_id}.csv"
            standard_csv_path = results_dir / "google_maps_data.csv"
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
        results_input = input("Enter path to results JSON file (e.g., results/google_maps_data.json): ").strip()
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

    deep_email_search = False
    if extract_emails:
        deep_email_search = input("   Enable deep email search (check contact pages)? (y/n, default: n): ").strip().lower() == 'y'
        print(f"   Deep email search: {'Yes' if deep_email_search else 'No'}")

    extract_reviews = input("Extract review snippets? (y/n, default: y): ").strip().lower() != 'n'
    print(f"   Extract reviews: {'Yes' if extract_reviews else 'No'}")

    try:
        workers_input = input(f"Number of parallel workers? (1-{os.cpu_count()*5 or 10}, default: 5): ").strip() # Default 5, suggest based on CPU
        max_workers = int(workers_input) if workers_input else 5
        max_workers = max(1, min(os.cpu_count()*10 or 20, max_workers)) # Limit workers reasonably
    except ValueError:
        max_workers = 5
    print(f"   Parallel workers: {max_workers}")
    
    # Advanced options
    print("\n--- Advanced Options (Optional) ---")
    driver_path = input("ChromeDriver path (optional): ").strip() or None
    chrome_binary = input("Chrome binary path (optional): ").strip() or None
    user_data_dir = input("User data directory (optional): ").strip() or None
    
    if driver_path:
        print(f"   Using ChromeDriver: {driver_path}")
    if chrome_binary: 
        print(f"   Using Chrome binary: {chrome_binary}")
    if user_data_dir:
        print(f"   Using user data dir: {user_data_dir}")

    # --- Initialize and Run ---
    scraper = None
    try:
        print("\nInitializing scraper...")
        scraper = GoogleMapsGridScraper(
            headless=headless_mode,
            max_workers=max_workers,
            debug=debug_mode,
            no_images=debug_mode is False, # Disable images if not debugging by default
            user_data_dir=user_data_dir,
            driver_path=driver_path,
            chrome_binary=chrome_binary
        )
        scraper.config["extract_emails"] = extract_emails
        scraper.config["deep_email_search"] = deep_email_search
        scraper.config["grid_size_meters"] = grid_size
        scraper.config["extract_reviews"] = extract_reviews

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
            session_csv_path = results_dir / f"google_maps_data_{scraper.session_id}.csv"
            standard_csv_path = results_dir / "google_maps_data.csv"
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
