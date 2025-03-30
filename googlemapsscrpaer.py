from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import csv
import json
import logging
import os
import re
import math
import concurrent.futures
from datetime import datetime
import threading
from urllib.parse import quote
import sys
import traceback
import random

# Try to import pandas for Excel export (optional)
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    print("Pandas not available. Will save as CSV instead of Excel.")

# Create directories
os.makedirs("logs", exist_ok=True)
os.makedirs("debug", exist_ok=True)
os.makedirs("results", exist_ok=True)

# Set up logging with color
class ColoredFormatter(logging.Formatter):
    """Custom formatter with colors"""
    grey = "\x1b[38;20m"
    green = "\x1b[32;20m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    
    FORMATS = {
        logging.DEBUG: grey + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.INFO: green + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.WARNING: yellow + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.ERROR: red + "%(asctime)s - %(levelname)s - %(message)s" + reset,
        logging.CRITICAL: bold_red + "%(asctime)s - %(levelname)s - %(message)s" + reset
    }
    
    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# Set up console handler with color formatter
console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter())

# Set up file handler
log_filename = f"logs/gmaps_grid_scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
file_handler = logging.FileHandler(log_filename)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))

# Set up grid debug logger
grid_log_filename = f"logs/grid_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
grid_handler = logging.FileHandler(grid_log_filename)
grid_handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

# Configure main logger
logger = logging.getLogger("GoogleMapsScraper")
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)
logger.addHandler(file_handler)

# Configure grid logger
grid_logger = logging.getLogger("GridDebug")
grid_logger.setLevel(logging.DEBUG)
grid_logger.addHandler(grid_handler)

class EnhancedGoogleMapsGridScraper:
    def __init__(self, headless=False, max_tabs=3, debug=True):
        """Initialize the Enhanced Google Maps Grid Scraper"""
        logger.info("🚀 Setting up Enhanced Google Maps Grid Scraper...")
        
        self.debug = debug
        self.headless = headless
        
        # Set up Chrome options
        self.options = Options()
        self.options.add_argument("--window-size=1920,1080")
        
        if headless:
            logger.info("Running in headless mode")
            self.options.add_argument("--headless=new")
        else:
            logger.info("Running with visible browser")
            self.options.add_argument("--start-maximized")
        
        # Performance optimizations
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-extensions")
        self.options.add_argument("--disable-gpu")
        self.options.add_argument("--disable-infobars")
        self.options.add_argument("--disable-notifications")
        
        # Add random user agent
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.2 Safari/605.1.15",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        ]
        self.options.add_argument(f"user-agent={random.choice(user_agents)}")
        
        # Initialize Chrome
        try:
            logger.info("Initializing Chrome browser...")
            print("Initializing Chrome browser... (this might take a few seconds)")
            
            self.driver = webdriver.Chrome(options=self.options)
            
            # Set longer timeouts for stability
            self.driver.set_page_load_timeout(30)
            self.driver.set_script_timeout(30)
            
            logger.info("✅ Chrome browser initialized successfully")
            print("✅ Chrome browser initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize Chrome: {e}")
            print(f"❌ ERROR: Failed to initialize Chrome: {e}")
            print("Please make sure Chrome and chromedriver are properly installed.")
            sys.exit(1)
        
        # Store results
        self.results = []
        self.processed_links = set()  # To track already processed links
        self.seen_businesses = {}  # name+address -> index in results
        
        # Grid info
        self.grid = []
        self.current_grid_cell = None
        
        # Max tabs for parallel processing
        self.max_tabs = max_tabs
        
        # For saving data
        self.lock = threading.Lock()
        self.session_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Stats for tracking progress
        self.stats = {
            "grid_cells_total": 0,
            "grid_cells_processed": 0,
            "grid_cells_empty": 0,
            "businesses_found": 0,
            "start_time": None,
            "processed_urls": 0
        }
        
    def handle_consent_pages(self):
        """Handle various Google consent pages"""
        try:
            # Check if we're on a consent page
            current_url = self.driver.current_url
            if "consent.google.com" in current_url:
                logger.info(f"⚠️ Detected consent page: {current_url}")
                print(f"⚠️ Detected consent page")
                
                # Take a screenshot for debugging
                if self.debug:
                    self.driver.save_screenshot("debug/consent_page.png")
                    logger.info("Saved consent page screenshot to debug/consent_page.png")
                
                # Method 1: Try to click "Accept all" button
                try:
                    consent_buttons = self.driver.find_elements(By.XPATH, "//button[contains(., 'Accept all')]")
                    if consent_buttons:
                        consent_buttons[0].click()
                        logger.info("Clicked 'Accept all' button")
                        time.sleep(2)
                        return True
                except Exception as e:
                    logger.warning(f"Method 1 failed: {e}")
                
                # Method 2: Try to click "I agree" button
                try:
                    agree_buttons = self.driver.find_elements(By.XPATH, "//button[contains(., 'I agree')]")
                    if agree_buttons:
                        agree_buttons[0].click()
                        logger.info("Clicked 'I agree' button")
                        time.sleep(2)
                        return True
                except Exception as e:
                    logger.warning(f"Method 2 failed: {e}")
                
                # Method 3: Try to use form submission
                try:
                    # Find form buttons and click the first one (usually Accept)
                    form_buttons = self.driver.find_elements(By.CSS_SELECTOR, "form button")
                    if form_buttons:
                        form_buttons[0].click()
                        logger.info("Clicked first form button")
                        time.sleep(2)
                        return True
                except Exception as e:
                    logger.warning(f"Method 3 failed: {e}")
                
                # If not headless, ask for manual intervention
                if not self.headless:
                    logger.warning("Could not handle consent page automatically")
                    print("\n============= MANUAL INTERVENTION REQUIRED =============")
                    print("Please accept the consent in the browser window manually")
                    print("You have 20 seconds to accept the consent...")
                    print("============= MANUAL INTERVENTION REQUIRED =============\n")
                    time.sleep(20)  # Give user time to accept
                    return True
                
                logger.error("❌ Failed to handle consent page")
                return False
            
            # Also check for other consent banners directly on Google Maps
            try:
                # Common cookie/consent banners
                consent_selectors = [
                    "button#L2AGLb",  # Google cookie consent
                    "button.tHlp8d",  # Another Google consent button
                    "div.VfPpkd-dgl2Hf-ppHlrf-sM5MNb button",  # Material design buttons
                ]
                
                for selector in consent_selectors:
                    elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                    if elements:
                        elements[0].click()
                        logger.info(f"Clicked consent button: {selector}")
                        time.sleep(1)
                        return True
            except Exception as e:
                logger.warning(f"Error handling cookie banner: {e}")
            
            return False
            
        except Exception as e:
            logger.error(f"Error in consent handling: {e}")
            return False

    def get_exact_city_bounds(self, location):
        """Get precise bounding box for a city by finding its extreme points"""
        logger.info(f"📍 Finding precise boundaries for location: {location}")
        print(f"Finding precise boundaries for {location}...")
        
        try:
            # Go to Google Maps
            self.driver.get("https://www.google.com/maps")
            time.sleep(3)
            
            # Handle consent page if it appears
            self.handle_consent_pages()
            
            # Search for the location
            search_box = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "searchboxinput"))
            )
            search_box.clear()
            search_box.send_keys(location)
            search_box.send_keys(Keys.ENTER)
            logger.info(f"Searched for location: {location}")
            
            # Wait for the result and for map to load
            time.sleep(5)
            
            # Take screenshot for debugging
            if self.debug:
                self.driver.save_screenshot("debug/location_search.png")
            
            # Use JavaScript to get map bounds
            bounds_data = self.driver.execute_script("""
                // Try to get Google Maps map instance
                let mapInstance;
                
                // Method 1: Direct map access
                if (window.google && window.google.maps) {
                    const maps = Array.from(document.querySelectorAll('*')).filter(el => el.__gm);
                    if (maps.length > 0) {
                        mapInstance = maps[0].__gm.map;
                    }
                }
                
                // Method 2: If map not found, try to extract from URL
                if (!mapInstance) {
                    const url = window.location.href;
                    const match = url.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+),(-?\\d+\\.?\\d*)z/);
                    if (match) {
                        // Approximate bounds based on center and zoom
                        const center = { lat: parseFloat(match[1]), lng: parseFloat(match[2]) };
                        const zoom = parseFloat(match[3]);
                        
                        // Calculate approximate bounds based on zoom level
                        // At zoom level 15, roughly 2km view
                        const zoomFactor = Math.pow(2, 15 - zoom);
                        const latDelta = 0.018 * zoomFactor;  // ~2km at zoom 15
                        const lngDelta = 0.018 * zoomFactor * 
                            Math.cos(center.lat * Math.PI / 180);  // Adjust for latitude
                        
                        return {
                            northeast: { lat: center.lat + latDelta, lng: center.lng + lngDelta },
                            southwest: { lat: center.lat - latDelta, lng: center.lng - lngDelta },
                            center: center,
                            zoom: zoom,
                            method: 'url-estimation'
                        };
                    }
                }
                
                // Method 3: Try to find visible area bounds
                if (mapInstance && mapInstance.getBounds) {
                    const bounds = mapInstance.getBounds();
                    const center = mapInstance.getCenter();
                    const zoom = mapInstance.getZoom();
                    
                    return {
                        northeast: { 
                            lat: bounds.getNorthEast().lat(), 
                            lng: bounds.getNorthEast().lng() 
                        },
                        southwest: { 
                            lat: bounds.getSouthWest().lat(), 
                            lng: bounds.getSouthWest().lng() 
                        },
                        center: { lat: center.lat(), lng: center.lng() },
                        zoom: zoom,
                        method: 'map-bounds'
                    };
                }
                
                // Fallback: Extract from URL
                const url = window.location.href;
                const match = url.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+),(-?\\d+\\.?\\d*)z/);
                if (match) {
                    // Center point from URL
                    const lat = parseFloat(match[1]);
                    const lng = parseFloat(match[2]);
                    const zoom = parseFloat(match[3]);
                    
                    // Calculate approximate bounds based on zoom level
                    // At zoom level 12, roughly 10km view (good for cities)
                    const latDelta = 0.045;  // ~5km north/south from center
                    const lngDelta = 0.065;  // ~5km east/west from center
                    
                    return {
                        northeast: { lat: lat + latDelta, lng: lng + lngDelta },
                        southwest: { lat: lat - latDelta, lng: lng - lngDelta },
                        center: { lat: lat, lng: lng },
                        zoom: zoom,
                        method: 'fallback'
                    };
                }
                
                return null;
            """)
            
            if bounds_data:
                logger.info(f"Found city bounds: NE={bounds_data['northeast']}, SW={bounds_data['southwest']}")
                logger.info(f"Bounds detection method: {bounds_data.get('method', 'unknown')}")
                
                # Check if the bounds look reasonable
                ne = bounds_data['northeast']
                sw = bounds_data['southwest']
                
                # Calculate width and height in km
                lat_delta = ne['lat'] - sw['lat']
                lng_delta = ne['lng'] - sw['lng']
                
                # 1 degree latitude ≈ 111km
                # 1 degree longitude ≈ 111km * cos(latitude)
                avg_lat = (ne['lat'] + sw['lat']) / 2
                width_km = lng_delta * 111 * math.cos(math.radians(avg_lat))
                height_km = lat_delta * 111
                
                logger.info(f"Approximate city size: {width_km:.2f}km x {height_km:.2f}km")
                print(f"City boundaries detected: {width_km:.2f}km x {height_km:.2f}km")
                
                # Expand bounds by 5% to ensure we cover the entire city
                center_lat = (ne['lat'] + sw['lat']) / 2
                center_lng = (ne['lng'] + sw['lng']) / 2
                
                # Expand by 10% to ensure coverage
                expanded_lat_delta = lat_delta * 1.1
                expanded_lng_delta = lng_delta * 1.1
                
                expanded_bounds = {
                    'northeast': {
                        'lat': center_lat + expanded_lat_delta/2,
                        'lng': center_lng + expanded_lng_delta/2
                    },
                    'southwest': {
                        'lat': center_lat - expanded_lat_delta/2,
                        'lng': center_lng - expanded_lng_delta/2
                    },
                    'center': {
                        'lat': center_lat,
                        'lng': center_lng
                    },
                    'width_km': width_km,
                    'height_km': height_km
                }
                
                logger.info(f"Expanded bounds by 10%: NE={expanded_bounds['northeast']}, SW={expanded_bounds['southwest']}")
                
                return expanded_bounds
            else:
                logger.warning("Could not determine city bounds via JavaScript")
                
                # Fallback: extract from URL
                url = self.driver.current_url
                match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+),(\d+\.?\d*)z', url)
                
                if match:
                    lat = float(match.group(1))
                    lng = float(match.group(2))
                    zoom = float(match.group(3))
                    
                    # Use approximate calculation for bounds
                    # At zoom level 12, roughly 20km view
                    lat_delta = 0.09  # ~10km north/south
                    lng_delta = 0.12  # ~10km east/west at middle latitudes
                    
                    bounds = {
                        'northeast': {'lat': lat + lat_delta, 'lng': lng + lng_delta},
                        'southwest': {'lat': lat - lat_delta, 'lng': lng - lng_delta},
                        'center': {'lat': lat, 'lng': lng},
                        'width_km': lng_delta * 111 * math.cos(math.radians(lat)),
                        'height_km': lat_delta * 111
                    }
                    
                    logger.info(f"Fallback bounds: NE={bounds['northeast']}, SW={bounds['southwest']}")
                    print(f"Estimated city size: {bounds['width_km']:.2f}km x {bounds['height_km']:.2f}km")
                    
                    return bounds
                
                logger.error("Could not determine city bounds")
                print("❌ Could not determine city boundaries")
                return None
                
        except Exception as e:
            logger.error(f"Error getting city bounds: {e}")
            print(f"❌ Error getting city boundaries: {e}")
            
            # In case of error, provide a very general fallback
            if "prague" in location.lower():
                # Prague fallback coordinates
                return {
                    'northeast': {'lat': 50.1291, 'lng': 14.5656},
                    'southwest': {'lat': 49.9425, 'lng': 14.2244},
                    'center': {'lat': 50.0755, 'lng': 14.4378},
                    'width_km': 22.5,
                    'height_km': 20.8
                }
            
            return None
    
    def create_optimal_grid(self, bounds, grid_size_meters=250):
        """Create an optimal grid based on city bounds"""
        logger.info(f"📊 Creating grid with {grid_size_meters}m cells")
        print(f"Creating grid with {grid_size_meters}m cells...")
        
        grid_logger.debug("=== STARTING GRID CREATION ===")
        grid_logger.debug(f"City bounds: NE={bounds['northeast']}, SW={bounds['southwest']}")
        grid_logger.debug(f"Area size: {bounds.get('width_km', 'unknown')}km x {bounds.get('height_km', 'unknown')}km")
        grid_logger.debug(f"Grid cell size: {grid_size_meters} meters")
        
        # Get bounds coordinates
        ne_lat = bounds['northeast']['lat']
        ne_lng = bounds['northeast']['lng']
        sw_lat = bounds['southwest']['lat']
        sw_lng = bounds['southwest']['lng']
        
        # Calculate grid dimensions
        # 1 degree latitude ≈ 111km
        # 1 degree longitude ≈ 111km * cos(latitude)
        avg_lat = (ne_lat + sw_lat) / 2
        meters_per_degree_lat = 111000  # Approximate
        meters_per_degree_lng = 111000 * math.cos(math.radians(avg_lat))  # Adjust for latitude
        
        # Calculate grid cell size in degrees
        grid_size_lat = grid_size_meters / meters_per_degree_lat
        grid_size_lng = grid_size_meters / meters_per_degree_lng
        
        grid_logger.debug(f"Grid cell size in degrees: lat={grid_size_lat:.6f}, lng={grid_size_lng:.6f}")
        
        # Calculate number of cells in each direction
        lat_span = ne_lat - sw_lat
        lng_span = ne_lng - sw_lng
        
        cells_lat = math.ceil(lat_span / grid_size_lat)
        cells_lng = math.ceil(lng_span / grid_size_lng)
        
        grid_logger.debug(f"Number of cells: {cells_lat} rows x {cells_lng} columns = {cells_lat * cells_lng} total cells")
        
        # Create the grid
        grid = []
        for i in range(cells_lat):
            lat1 = sw_lat + (i * grid_size_lat)
            lat2 = sw_lat + ((i + 1) * grid_size_lat)
            
            for j in range(cells_lng):
                lng1 = sw_lng + (j * grid_size_lng)
                lng2 = sw_lng + ((j + 1) * grid_size_lng)
                
                # Create cell
                cell = {
                    "southwest": {"lat": lat1, "lng": lng1},
                    "northeast": {"lat": lat2, "lng": lng2},
                    "center": {
                        "lat": (lat1 + lat2) / 2,
                        "lng": (lng1 + lng2) / 2
                    },
                    "row": i,
                    "col": j,
                    "cell_id": f"r{i}c{j}",
                    "likely_empty": False,
                    "processed": False
                }
                
                grid.append(cell)
                grid_logger.debug(f"Created cell {len(grid)}: ID=r{i}c{j}, SW=[{lat1:.6f},{lng1:.6f}], NE=[{lat2:.6f},{lng2:.6f}]")
        
        # Count total cells
        total_cells = len(grid)
        
        logger.info(f"Created grid with {total_cells} cells ({cells_lat} rows x {cells_lng} columns)")
        print(f"Created grid with {total_cells} cells ({cells_lat} rows x {cells_lng} columns)")
        
        grid_logger.debug("=== GRID CREATION COMPLETE ===")
        
        # Save grid for reference
        grid_file = f"logs/grid_{self.session_id}.json"
        with open(grid_file, 'w') as f:
            json.dump(grid, f, indent=2)
        logger.info(f"Saved grid to {grid_file}")
        
        # Generate a visual representation of the grid
        self.generate_grid_visualization(grid, cells_lat, cells_lng)
        
        self.grid = grid
        self.stats["grid_cells_total"] = total_cells
        return grid
    
    def generate_grid_visualization(self, grid, rows, cols):
        """Generate a visual representation of the grid for better understanding"""
        grid_logger.debug("\nGrid Visual Representation:")
        
        # Create a 2D matrix representation of the grid
        matrix = [['   ' for _ in range(cols)] for _ in range(rows)]
        
        # Fill in the matrix
        for cell in grid:
            r = cell["row"]
            c = cell["col"]
            matrix[r][c] = f"{r},{c}"
        
        # Print the grid
        for r in range(rows):
            row_str = "|"
            for c in range(cols):
                row_str += f"{matrix[r][c]:^5}|"
            grid_logger.debug(row_str)
            
        # Also create an HTML visualization
        html_output = """
        <!DOCTYPE html>
        <html>
        <head>
            <title>Grid Visualization</title>
            <style>
                body { font-family: Arial, sans-serif; }
                .grid { display: table; border-collapse: collapse; margin: 20px; }
                .row { display: table-row; }
                .cell { 
                    display: table-cell; 
                    border: 1px solid #ccc; 
                    width: 60px; 
                    height: 30px; 
                    text-align: center; 
                    vertical-align: middle;
                    font-size: 12px;
                }
                .empty { background-color: #f8f8f8; }
                .processed { background-color: #d4f7d4; }
                .current { background-color: #ffdb99; }
                .header { 
                    font-weight: bold; 
                    background-color: #eee; 
                    text-align: center;
                }
                .legend {
                    margin: 20px;
                    padding: 10px;
                    border: 1px solid #ccc;
                    display: inline-block;
                }
                .legend-item {
                    margin: 5px;
                }
                .legend-box {
                    display: inline-block;
                    width: 20px;
                    height: 20px;
                    margin-right: 5px;
                    vertical-align: middle;
                }
            </style>
        </head>
        <body>
            <h1>Grid Visualization</h1>
            <div class="legend">
                <div class="legend-item">
                    <div class="legend-box" style="background-color: #f8f8f8;"></div>
                    <span>Empty Cell</span>
                </div>
                <div class="legend-item">
                    <div class="legend-box" style="background-color: #d4f7d4;"></div>
                    <span>Processed Cell</span>
                </div>
                <div class="legend-item">
                    <div class="legend-box" style="background-color: #ffdb99;"></div>
                    <span>Current Cell</span>
                </div>
            </div>
            <div class="grid">
        """
        
        # Add column headers
        html_output += "<div class='row'><div class='cell header'></div>"
        for c in range(cols):
            html_output += f"<div class='cell header'>{c}</div>"
        html_output += "</div>"
        
        # Add rows
        for r in range(rows):
            html_output += f"<div class='row'><div class='cell header'>{r}</div>"
            for c in range(cols):
                cell_class = "cell"
                if r < len(matrix) and c < len(matrix[r]):
                    cell_content = f"r{r}c{c}"
                else:
                    cell_content = ""
                html_output += f"<div class='{cell_class}'>{cell_content}</div>"
            html_output += "</div>"
        
        html_output += """
            </div>
        </body>
        </html>
        """
        
        # Save HTML visualization
        with open(f"logs/grid_visualization_{self.session_id}.html", "w") as f:
            f.write(html_output)
        
        logger.info(f"Saved grid visualization to logs/grid_visualization_{self.session_id}.html")
    
    def update_grid_visualization(self):
        """Update the grid visualization with current progress"""
        if not self.grid:
            return
        
        try:
            # Get grid dimensions
            rows = max(cell["row"] for cell in self.grid) + 1
            cols = max(cell["col"] for cell in self.grid) + 1
            
            # Create a mapping of cell IDs to their status
            cell_status = {}
            for cell in self.grid:
                status = "empty" if cell.get("likely_empty", False) else ""
                status = "processed" if cell.get("processed", False) else status
                status = "current" if cell.get("cell_id") == (self.current_grid_cell or {}).get("cell_id") else status
                cell_status[cell["cell_id"]] = status
            
            # Create the HTML
            html_output = """
            <!DOCTYPE html>
            <html>
            <head>
                <meta http-equiv="refresh" content="30">
                <title>Grid Progress</title>
                <style>
                    body { font-family: Arial, sans-serif; }
                    .grid { display: table; border-collapse: collapse; margin: 20px; }
                    .row { display: table-row; }
                    .cell { 
                        display: table-cell; 
                        border: 1px solid #ccc; 
                        width: 60px; 
                        height: 30px; 
                        text-align: center; 
                        vertical-align: middle;
                        font-size: 12px;
                    }
                    .empty { background-color: #f8f8f8; }
                    .processed { background-color: #d4f7d4; }
                    .current { background-color: #ffdb99; }
                    .header { 
                        font-weight: bold; 
                        background-color: #eee; 
                        text-align: center;
                    }
                    .legend {
                        margin: 20px;
                        padding: 10px;
                        border: 1px solid #ccc;
                        display: inline-block;
                    }
                    .legend-item {
                        margin: 5px;
                    }
                    .legend-box {
                        display: inline-block;
                        width: 20px;
                        height: 20px;
                        margin-right: 5px;
                        vertical-align: middle;
                    }
                    .stats {
                        margin: 20px;
                        padding: 10px;
                        border: 1px solid #ccc;
                    }
                </style>
            </head>
            <body>
                <h1>Grid Progress</h1>
                <div class="stats">
                    <h2>Scraping Statistics</h2>
                    <p>Total Cells: {total_cells}</p>
                    <p>Processed Cells: {processed_cells}</p>
                    <p>Empty Cells: {empty_cells}</p>
                    <p>Businesses Found: {businesses_found}</p>
                    <p>Time Elapsed: {time_elapsed}</p>
                </div>
                <div class="legend">
                    <div class="legend-item">
                        <div class="legend-box" style="background-color: #f8f8f8;"></div>
                        <span>Empty Cell</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-box" style="background-color: #d4f7d4;"></div>
                        <span>Processed Cell</span>
                    </div>
                    <div class="legend-item">
                        <div class="legend-box" style="background-color: #ffdb99;"></div>
                        <span>Current Cell</span>
                    </div>
                </div>
                <div class="grid">
            """.format(
                total_cells=self.stats["grid_cells_total"],
                processed_cells=self.stats["grid_cells_processed"],
                empty_cells=self.stats["grid_cells_empty"],
                businesses_found=self.stats["businesses_found"],
                time_elapsed=self.get_elapsed_time()
            )
            
            # Add column headers
            html_output += "<div class='row'><div class='cell header'></div>"
            for c in range(cols):
                html_output += f"<div class='cell header'>{c}</div>"
            html_output += "</div>"
            
            # Add rows
            for r in range(rows):
                html_output += f"<div class='row'><div class='cell header'>{r}</div>"
                for c in range(cols):
                    cell_id = f"r{r}c{c}"
                    status = cell_status.get(cell_id, "")
                    cell_class = f"cell {status}" if status else "cell"
                    html_output += f"<div class='{cell_class}'>{cell_id}</div>"
                html_output += "</div>"
            
            html_output += """
                </div>
            </body>
            </html>
            """
            
            # Save updated visualization
            with open(f"logs/grid_progress_{self.session_id}.html", "w") as f:
                f.write(html_output)
            
        except Exception as e:
            logger.warning(f"Error updating grid visualization: {e}")
    
    def get_elapsed_time(self):
        """Get elapsed time in human-readable format"""
        if not self.stats["start_time"]:
            return "00:00:00"
        
        elapsed_seconds = (datetime.now() - self.stats["start_time"]).total_seconds()
        hours = int(elapsed_seconds // 3600)
        minutes = int((elapsed_seconds % 3600) // 60)
        seconds = int(elapsed_seconds % 60)
        
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
    
    def search_in_grid_cell(self, query, grid_cell):
        """Search for places in a specific grid cell"""
        self.current_grid_cell = grid_cell
        cell_id = grid_cell["cell_id"]
        center = grid_cell["center"]
        
        logger.info(f"🔍 Searching in grid cell {cell_id} at coordinates {center['lat']:.6f}, {center['lng']:.6f}")
        print(f"Searching grid cell {cell_id} at coordinates {center['lat']:.6f}, {center['lng']:.6f}")
        
        # Update grid visualization
        self.update_grid_visualization()
        
        try:
            # Format search query with coordinates - use direct URL
            url = f"https://www.google.com/maps/search/{quote(query)}/@{center['lat']},{center['lng']},17z"
            logger.info(f"Grid cell {cell_id} URL: {url}")
            
            # Navigate to the URL
            self.driver.get(url)
            time.sleep(3)
            
            # Handle consent page if needed
            self.handle_consent_pages()
            
            # Take screenshot for debugging
            if self.debug:
                screenshot_path = f"debug/grid_cell_{cell_id}.png"
                self.driver.save_screenshot(screenshot_path)
            
            # Wait for results to load
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "div[role='feed'], div.section-result-content"))
                )
                logger.info(f"Search results loaded for grid cell {cell_id}")
            except TimeoutException:
                logger.warning(f"Timeout waiting for search results in grid cell {cell_id}")
                time.sleep(3)  # Wait a bit more just in case
            
            # Extract business links
            business_links = []
            
            # Get visible links first
            visible_links = self.extract_visible_links()
            if visible_links:
                business_links.extend(visible_links)
            
            # Then scroll to find more
            scroll_links = self.scroll_and_collect_links()
            if scroll_links:
                # Add new links not already in business_links
                for link in scroll_links:
                    if link not in business_links:
                        business_links.append(link)
            
            if not business_links:
                logger.warning(f"No business links found in grid cell {cell_id}")
                print(f"No businesses found in grid cell {cell_id}")
                
                # Mark cell as processed
                grid_cell["processed"] = True
                self.stats["grid_cells_processed"] += 1
                
                return []
            
            logger.info(f"Found {len(business_links)} unique business links in grid cell {cell_id}")
            print(f"Found {len(business_links)} businesses in grid cell {cell_id}")
            
            # Mark cell as processed
            grid_cell["processed"] = True
            self.stats["grid_cells_processed"] += 1
            
            # Update grid visualization
            self.update_grid_visualization()
            
            return business_links
            
        except Exception as e:
            logger.error(f"Error searching in grid cell {cell_id}: {e}")
            print(f"❌ Error searching in grid cell {cell_id}: {e}")
            
            # Mark cell as processed despite error
            grid_cell["processed"] = True
            self.stats["grid_cells_processed"] += 1
            
            # Update grid visualization
            self.update_grid_visualization()
            
            return []
    
    def extract_visible_links(self):
        """Extract visible business links without scrolling"""
        try:
            # Use JavaScript to find all visible links at the current scroll position
            links = self.driver.execute_script("""
                const links = new Set();
                
                // Method 1: Find all links to place pages
                document.querySelectorAll('a[href*="/maps/place/"]').forEach(el => {
                    if (el.href && el.href.includes('/maps/place/')) {
                        links.add(el.href);
                    }
                });
                
                // Method 2: Look for business cards and the links inside them
                const businessCards = document.querySelectorAll('div[role="article"], div.Nv2PK, div.bfdHYd');
                businessCards.forEach(card => {
                    const links = card.querySelectorAll('a[href*="/maps/place/"]');
                    links.forEach(link => {
                        if (link.href) {
                            links.add(link.href);
                        }
                    });
                });
                
                return Array.from(links);
            """)
            
            if links:
                logger.info(f"Extracted {len(links)} visible business links")
                return links
            
            return []
        except Exception as e:
            logger.warning(f"Error extracting visible links: {e}")
            return []
    
    def scroll_and_collect_links(self, max_scrolls=10):
        """Scroll through results and collect business links"""
        links_found = set()
        prev_len = 0
        stagnant_count = 0
        
        # Find the scrollable container
        scroll_element = None
        selectors = [
            "div[role='feed']",
            "div.section-scrollbox",
            "div.DxyBCb",
            "div.m6QErb"
        ]
        
        for selector in selectors:
            try:
                elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    scroll_element = elements[0]
                    logger.info(f"Found scrollable container with selector: {selector}")
                    break
            except:
                continue
        
        if not scroll_element:
            logger.warning("Could not find scrollable element, using body")
            scroll_element = self.driver.find_element(By.TAG_NAME, "body")
        
        # Scroll and collect links
        for i in range(max_scrolls):
            # Scroll down
            try:
                self.driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scroll_element)
            except:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            
            time.sleep(1)
            
            # Extract links using JavaScript
            new_links = self.driver.execute_script("""
                const links = [];
                
                // Find all links to place pages
                document.querySelectorAll('a[href*="/maps/place/"]').forEach(el => {
                    if (el.href && el.href.includes('/maps/place/')) {
                        links.push(el.href);
                    }
                });
                
                return links;
            """)
            
            # Add new links
            original_count = len(links_found)
            for link in new_links:
                if link not in links_found:
                    links_found.add(link)
            
            # Check if we found new links
            current_count = len(links_found)
            if current_count == original_count:
                stagnant_count += 1
                if stagnant_count >= 3:  # No new links after 3 scrolls
                    break
            else:
                stagnant_count = 0
        
        return list(links_found)
    
    def extract_place_info(self, url):
        """Extract business information from a Google Maps URL"""
        if url in self.processed_links:
            logger.info(f"Skipping already processed URL: {url}")
            return None
        
        self.processed_links.add(url)
        self.stats["processed_urls"] += 1
        logger.info(f"Processing URL: {url}")
        
        # Create place info dictionary
        place_info = {
            "name": "",
            "address": "",
            "phone": "",
            "website": "",
            "maps_url": url,
            "rating": "",
            "reviews_count": "",
            "category": "",
            "hours": "",
            "email": "",
            "location": "",
            "grid_cell": self.current_grid_cell["cell_id"] if self.current_grid_cell else ""
        }
        
        try:
            # Load the business page
            self.driver.get(url)
            time.sleep(3)
            
            # Handle consent page if it appears
            self.handle_consent_pages()
            
            # Take screenshot for debugging
            if self.debug and (len(self.results) % 20 == 0):  # Only save every 20th business for space
                screenshot_path = f"debug/business_{len(self.results) + 1}.png"
                self.driver.save_screenshot(screenshot_path)
            
            # Extract information using JavaScript (most reliable method)
            js_data = self.driver.execute_script("""
                function extractBusinessInfo() {
                    const data = {
                        name: "",
                        address: "",
                        phone: "",
                        website: "",
                        rating: "",
                        reviews_count: "",
                        category: "",
                        hours: ""
                    };
                    
                    // Extract name (h1/h2 heading)
                    const headings = document.querySelectorAll('h1, h2[role="heading"]');
                    for (const heading of headings) {
                        if (heading.textContent && heading.textContent.trim().length > 0) {
                            data.name = heading.textContent.trim();
                            break;
                        }
                    }
                    
                    // Extract address
                    const addressElements = [
                        ...document.querySelectorAll('button[data-item-id^="address"] div'),
                        ...document.querySelectorAll('button[data-tooltip="Copy address"] span'),
                        ...document.querySelectorAll('button[aria-label*="address"]'),
                        ...document.querySelectorAll('button[aria-label*="location"]')
                    ];
                    
                    for (const el of addressElements) {
                        if (el.textContent && el.textContent.trim().length > 5) {
                            data.address = el.textContent.trim();
                            break;
                        }
                    }
                    
                    // Extract phone
                    const phoneElements = [
                        ...document.querySelectorAll('button[data-item-id^="phone"] div'),
                        ...document.querySelectorAll('button[data-tooltip*="phone"] span'),
                        ...document.querySelectorAll('button[aria-label*="phone"]'),
                        ...document.querySelectorAll('a[href^="tel:"]')
                    ];
                    
                    for (const el of phoneElements) {
                        const text = el.textContent || "";
                        if (text.trim() && /\\d/.test(text)) {
                            data.phone = text.trim();
                            break;
                        }
                    }
                    
                    // Extract website
                    const websiteElements = [
                        ...document.querySelectorAll('a[data-item-id^="authority"]'),
                        ...document.querySelectorAll('a[data-tooltip="Open website"]'),
                        ...document.querySelectorAll('a[jsaction*="website"]'),
                        ...document.querySelectorAll('a[href*="http"]:not([href*="google"])')
                    ];
                    
                    for (const el of websiteElements) {
                        if (el.href && !el.href.includes('google.com')) {
                            data.website = el.href;
                            break;
                        }
                    }
                    
                    // Extract rating
                    const ratingElements = [
                        ...document.querySelectorAll('span.kvMYJc'),
                        ...document.querySelectorAll('span[role="img"]'),
                        ...document.querySelectorAll('div.F7nice')
                    ];
                    
                    for (const el of ratingElements) {
                        const ratingText = el.textContent || el.getAttribute('aria-label') || "";
                        if (ratingText && /[0-9]/.test(ratingText)) {
                            const parts = ratingText.trim().split(/\\s+/);
                            if (parts.length >= 1) {
                                data.rating = parts[0];
                            }
                            if (parts.length >= 2) {
                                data.reviews_count = parts[1].replace(/[()]/g, '');
                            }
                            break;
                        }
                    }
                    
                    // Extract category
                    const categoryElements = [
                        ...document.querySelectorAll('button[jsaction*="category"]'),
                        ...document.querySelectorAll('span.section-rating-term'),
                        ...document.querySelectorAll('div.category-label'),
                        ...document.querySelectorAll('button[jsaction="pane.rating.category"]')
                    ];
                    
                    for (const el of categoryElements) {
                        const text = el.textContent ? el.textContent.trim() : "";
                        if (text && !text.startsWith('(') && !/^\\d/.test(text)) {
                            data.category = text;
                            break;
                        }
                    }
                    
                    // Extract hours
                    const hoursElements = [
                        ...document.querySelectorAll('div[jsaction*="openhours"]'),
                        ...document.querySelectorAll('span[aria-label*="hour"]'),
                        ...document.querySelectorAll('div.section-open-hours-container'),
                        ...document.querySelectorAll('table')
                    ];
                    
                    for (const el of hoursElements) {
                        const text = el.textContent ? el.textContent.trim() : "";
                        if (text && (text.includes('day') || text.includes('open') || text.includes('close'))) {
                            data.hours = text;
                            break;
                        }
                    }
                    
                    return data;
                }
                
                return extractBusinessInfo();
            """)
            
            # Update place_info with JavaScript results
            if js_data:
                for key, value in js_data.items():
                    if value and key in place_info:
                        place_info[key] = value
                        logger.info(f"JS extracted {key}: {value}")
                
                # Also set location
                if js_data.get('address'):
                    place_info["location"] = js_data['address']
            
            # If no name found, try traditional extraction
            if not place_info["name"]:
                try:
                    # Try multiple selectors for name
                    name_selectors = [
                        "h1.DUwDvf", 
                        "h1[role='heading']",
                        "h2[role='heading']",
                        "div.fontHeadlineLarge"
                    ]
                    
                    for selector in name_selectors:
                        name_elements = self.driver.find_elements(By.CSS_SELECTOR, selector)
                        if name_elements:
                            place_info["name"] = name_elements[0].text.strip()
                            logger.info(f"Traditional extraction - name: {place_info['name']}")
                            break
                except Exception as e:
                    logger.warning(f"Error extracting name: {e}")
            
            # If still no name, skip this result
            if not place_info["name"]:
                logger.warning("No name found, skipping")
                return None
            
            # Extract email if website is available
            if place_info["website"]:
                email = self.extract_email(place_info["website"])
                if email:
                    place_info["email"] = email
            
            # Check if we've already seen this business
            business_key = (place_info["name"], place_info.get("address", ""))
            
            if business_key in self.seen_businesses:
                logger.info(f"Duplicate business: {place_info['name']} (already seen)")
                
                # Check if we have email now but didn't before
                if place_info.get("email") and not self.results[self.seen_businesses[business_key]].get("email"):
                    self.results[self.seen_businesses[business_key]]["email"] = place_info["email"]
                    logger.info(f"Updated email for existing business: {place_info['name']}")
                
                return None
            
            # Mark as seen
            self.seen_businesses[business_key] = len(self.results)
            
            logger.info(f"Successfully extracted info for: {place_info['name']}")
            return place_info
            
        except Exception as e:
            logger.error(f"Error extracting place info: {e}")
            return None
    
    def extract_email(self, website_url):
        """Extract email from website using a dedicated browser instance"""
        logger.info(f"Looking for email on: {website_url}")
        
        try:
            # Create a separate browser for email extraction
            options = Options()
            options.add_argument("--headless=new")  # Always use headless for email extraction
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            
            email_driver = webdriver.Chrome(options=options)
            email_driver.set_page_load_timeout(10)  # Short timeout for speed
            
            try:
                # Visit website
                email_driver.get(website_url)
                time.sleep(3)
                
                # Look for email pattern in page source
                page_source = email_driver.page_source
                email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
                emails = re.findall(email_pattern, page_source)
                
                # Try contact page if no emails found
                if not emails:
                    # Look for contact page links
                    contact_links = email_driver.find_elements(By.XPATH, 
                        "//a[contains(translate(text(), 'CONTACT', 'contact'), 'contact') or contains(@href, 'contact')]")
                    
                    if contact_links and len(contact_links) > 0:
                        try:
                            contact_href = contact_links[0].get_attribute('href')
                            logger.info(f"Checking contact page: {contact_href}")
                            
                            email_driver.get(contact_href)
                            time.sleep(2)
                            
                            contact_page_source = email_driver.page_source
                            contact_emails = re.findall(email_pattern, contact_page_source)
                            
                            if contact_emails:
                                emails.extend(contact_emails)
                        except Exception as e:
                            logger.warning(f"Error checking contact page: {e}")
                
                # Filter out common false positives
                valid_emails = []
                for email in emails:
                    # Skip emails that look like false positives
                    if any(fp in email.lower() for fp in ['example', 'youremail', '@site', '@domain', 'username']):
                        continue
                    valid_emails.append(email)
                
                if valid_emails:
                    logger.info(f"Found email: {valid_emails[0]}")
                    return valid_emails[0]
                
                logger.info("No valid email found")
                return ""
                
            except Exception as e:
                logger.warning(f"Error in email extraction: {e}")
                return ""
                
            finally:
                # Always close the email driver
                email_driver.quit()
                
        except Exception as e:
            logger.error(f"Error creating email browser: {e}")
            return ""
    
    def process_grid_cell(self, query, grid_cell):
        """Process a single grid cell completely"""
        cell_id = grid_cell["cell_id"]
        
        try:
            # Search in this grid cell
            business_links = self.search_in_grid_cell(query, grid_cell)
            
            if not business_links:
                logger.info(f"No businesses found in grid cell {cell_id}")
                return 0
            
            # Process each business link
            processed_count = 0
            for i, link in enumerate(business_links):
                logger.info(f"Processing business {i+1}/{len(business_links)} in grid cell {cell_id}")
                
                place_info = self.extract_place_info(link)
                if place_info:
                    with self.lock:
                        self.results.append(place_info)
                        processed_count += 1
                        self.stats["businesses_found"] += 1
                        logger.info(f"Added place #{len(self.results)}: {place_info['name']}")
                
                # Save results every 10 businesses
                if (len(self.results) % 10 == 0):
                    self.save_results()
                    self.update_grid_visualization()
            
            # Save results after processing the cell
            if processed_count > 0:
                self.save_results()
            
            return processed_count
            
        except Exception as e:
            logger.error(f"Error processing grid cell {cell_id}: {e}")
            traceback.print_exc()
            return 0
    
    def save_results(self):
        """Save results to files"""
        if not self.results:
            logger.warning("No results to save")
            return
        
        try:
            # Base filename with session ID
            base_filename = f"results/google_maps_data_{self.session_id}"
            
            # Save to CSV
            csv_filename = f"{base_filename}.csv"
            self.save_to_csv(csv_filename)
            
            # Save to JSON
            json_filename = f"{base_filename}.json"
            self.save_to_json(json_filename)
            
            # Also save to standard location
            self.save_to_csv("google_maps_data.csv")
            self.save_to_json("google_maps_data.json")
            
            # Count unique business names
            unique_names = len(set(r["name"] for r in self.results))
            
            logger.info(f"💾 Saved {len(self.results)} results ({unique_names} unique businesses)")
            
        except Exception as e:
            logger.error(f"Error saving results: {e}")
    
    def save_to_csv(self, filename="google_maps_data.csv"):
        """Save results to CSV file"""
        if not self.results:
            return
        
        try:
            # Preferred column order
            preferred_order = [
                "name", "category", "address", "location", "phone", 
                "email", "website", "maps_url", "rating", "reviews_count", 
                "hours", "grid_cell"
            ]
            
            # Get all fields
            all_fields = set()
            for result in self.results:
                all_fields.update(result.keys())
            
            # Create ordered fieldnames
            fieldnames = []
            for field in preferred_order:
                if field in all_fields:
                    fieldnames.append(field)
                    if field in all_fields:
                        all_fields.remove(field)
            fieldnames.extend(sorted(all_fields))
            
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for result in self.results:
                    writer.writerow(result)
            
            # If pandas is available, also save as Excel
            if PANDAS_AVAILABLE:
                excel_filename = filename.replace('.csv', '.xlsx')
                df = pd.DataFrame(self.results)
                
                # Reorder columns
                excel_columns = []
                for field in preferred_order:
                    if field in df.columns:
                        excel_columns.append(field)
                
                # Add remaining columns
                for col in df.columns:
                    if col not in excel_columns:
                        excel_columns.append(col)
                
                # Filter columns that actually exist in the dataframe
                excel_columns = [col for col in excel_columns if col in df.columns]
                
                df = df[excel_columns]
                df.to_excel(excel_filename, index=False)
                logger.info(f"Saved Excel version to {excel_filename}")
            
            logger.info(f"Results saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving CSV: {e}")
    
    def save_to_json(self, filename="google_maps_data.json"):
        """Save results to JSON file"""
        if not self.results:
            return
        
        try:
            with open(filename, 'w', encoding='utf-8') as jsonfile:
                json.dump(self.results, jsonfile, indent=4, ensure_ascii=False)
            
            logger.info(f"Results saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving JSON: {e}")
    
    def sort_grid_cells_by_density(self, grid):
        """Sort grid cells by likely density of businesses (center of city first)"""
        # If grid is small, don't bother sorting
        if len(grid) <= 4:
            return grid
        
        # Get grid dimensions
        rows = max(cell["row"] for cell in grid) + 1
        cols = max(cell["col"] for cell in grid) + 1
        
        # Find center of grid
        center_row = rows // 2
        center_col = cols // 2
        
        # Calculate Manhattan distance from center for each cell
        for cell in grid:
            row_distance = abs(cell["row"] - center_row)
            col_distance = abs(cell["col"] - center_col)
            cell["center_distance"] = row_distance + col_distance
        
        # Sort by distance from center (ascending)
        sorted_grid = sorted(grid, key=lambda x: x["center_distance"])
        
        logger.info("Sorted grid cells by distance from center")
        return sorted_grid
    
    def scrape(self, query, location, grid_size_meters=250, max_results=None):
        """Main method to scrape businesses using the enhanced grid approach"""
        self.stats["start_time"] = datetime.now()
        start_time = self.stats["start_time"]
        
        logger.info(f"🚀 STARTING ENHANCED GRID SCRAPING - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Query: '{query}' in '{location}'")
        logger.info(f"Grid size: {grid_size_meters} meters")
        logger.info(f"Max results: {max_results or 'unlimited'}")
        
        print("\n===== STARTING ENHANCED GOOGLE MAPS GRID SCRAPER =====")
        print(f"Search: '{query}' in '{location}'")
        print(f"Grid size: {grid_size_meters} meters")
        print(f"Max results: {max_results or 'unlimited'}")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=======================================================\n")
        
        try:
            # Step 1: Get precise boundaries for the location
            bounds = self.get_exact_city_bounds(location)
            if not bounds:
                logger.error("❌ Could not determine location boundaries")
                print("❌ Could not determine location boundaries")
                return []
            
            # Step 2: Create an optimal grid based on the location bounds
            grid = self.create_optimal_grid(bounds, grid_size_meters)
            
            if not grid:
                logger.error("❌ Failed to create grid")
                print("❌ Failed to create grid")
                return []
            
            # Step 3: Sort grid cells by likely business density (center of city first)
            grid = self.sort_grid_cells_by_density(grid)
            
            # Step 4: Process each grid cell
            total_cells = len(grid)
            self.stats["grid_cells_total"] = total_cells
            
            print(f"\nProcessing {total_cells} grid cells...")
            
            for i, cell in enumerate(grid):
                # Check if we've reached max results
                if max_results and len(self.results) >= max_results:
                    logger.info(f"Reached maximum results ({max_results}), stopping")
                    print(f"Reached maximum results ({max_results}), stopping")
                    break
                
                cell_id = cell["cell_id"]
                
                # Print progress
                print(f"\nProcessing grid cell {i+1}/{total_cells} ({cell_id})...")
                
                # Process this grid cell
                processed_count = self.process_grid_cell(query, cell)
                
                # Print progress stats
                if processed_count > 0:
                    print(f"Found {processed_count} businesses in grid cell {cell_id}")
                else:
                    print(f"No businesses found in grid cell {cell_id}")
                
                print(f"Total so far: {len(self.results)} businesses ({i+1}/{total_cells} cells processed)")
                
                # Update grid visualization
                self.update_grid_visualization()
                
                # Wait between cells to avoid rate limiting
                time.sleep(1)
            
            # Calculate stats
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60  # in minutes
            unique_businesses = len(set((r["name"], r.get("address", "")) for r in self.results))
            
            logger.info(f"✅ GRID SCRAPING COMPLETE")
            logger.info(f"Found {len(self.results)} businesses ({unique_businesses} unique)")
            logger.info(f"Processed {self.stats['grid_cells_processed']} grid cells")
            logger.info(f"Duration: {duration:.2f} minutes")
            
            print("\n===== GRID SCRAPING COMPLETE =====")
            print(f"✅ Found {len(self.results)} businesses ({unique_businesses} unique)")
            print(f"Processed {self.stats['grid_cells_processed']} grid cells")
            print(f"Duration: {duration:.2f} minutes")
            print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Final save
            self.save_results()
            
            return self.results
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Grid scraping interrupted by user")
            print("\n⚠️ Grid scraping interrupted by user")
            print("Saving collected data...")
            self.save_results()
            return self.results
            
        except Exception as e:
            logger.error(f"❌ Error during grid scraping: {e}")
            print(f"\n❌ Error during grid scraping: {e}")
            print("Saving any collected data...")
            traceback.print_exc()
            self.save_results()
            return self.results
    
    def load_and_resume(self, results_file, grid_file):
        """Load previous results and grid, and resume scraping from where it left off"""
        try:
            # Load results
            if os.path.exists(results_file):
                with open(results_file, 'r', encoding='utf-8') as f:
                    self.results = json.load(f)
                
                # Rebuild seen_businesses map
                for i, result in enumerate(self.results):
                    business_key = (result["name"], result.get("address", ""))
                    self.seen_businesses[business_key] = i
                    
                    # Add to processed links
                    if "maps_url" in result and result["maps_url"]:
                        self.processed_links.add(result["maps_url"])
                
                logger.info(f"Loaded {len(self.results)} businesses from {results_file}")
                print(f"Loaded {len(self.results)} businesses from {results_file}")
            else:
                logger.warning(f"Results file {results_file} not found")
                print(f"Results file {results_file} not found")
                return False
            
            # Load grid
            if os.path.exists(grid_file):
                with open(grid_file, 'r') as f:
                    self.grid = json.load(f)
                
                # Mark cells as processed if they have corresponding results
                processed_cells = set()
                for result in self.results:
                    if "grid_cell" in result and result["grid_cell"]:
                        processed_cells.add(result["grid_cell"])
                
                for cell in self.grid:
                    if cell["cell_id"] in processed_cells:
                        cell["processed"] = True
                
                self.stats["grid_cells_total"] = len(self.grid)
                self.stats["grid_cells_processed"] = sum(1 for cell in self.grid if cell.get("processed", False))
                self.stats["businesses_found"] = len(self.results)
                
                logger.info(f"Loaded grid with {len(self.grid)} cells from {grid_file}")
                logger.info(f"{self.stats['grid_cells_processed']} cells already processed")
                print(f"Loaded grid with {len(self.grid)} cells from {grid_file}")
                print(f"{self.stats['grid_cells_processed']} cells already processed")
                
                return True
            else:
                logger.warning(f"Grid file {grid_file} not found")
                print(f"Grid file {grid_file} not found")
                return False
            
        except Exception as e:
            logger.error(f"Error loading previous session: {e}")
            print(f"Error loading previous session: {e}")
            return False
    
    def resume_scraping(self, query, max_results=None):
        """Resume scraping from where it left off"""
        if not self.grid:
            logger.error("No grid loaded, cannot resume")
            print("No grid loaded, cannot resume")
            return []
        
        self.stats["start_time"] = datetime.now()
        start_time = self.stats["start_time"]
        
        logger.info(f"🚀 RESUMING GRID SCRAPING - {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"Query: '{query}'")
        logger.info(f"Max results: {max_results or 'unlimited'}")
        
        print("\n===== RESUMING GRID SCRAPING =====")
        print(f"Query: '{query}'")
        print(f"Max results: {max_results or 'unlimited'}")
        print(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print("===================================\n")
        
        try:
            # Filter unprocessed cells
            unprocessed_cells = [cell for cell in self.grid if not cell.get("processed", False)]
            
            if not unprocessed_cells:
                logger.info("All cells already processed, nothing to do")
                print("All cells already processed, nothing to do")
                return self.results
            
            logger.info(f"Resuming with {len(unprocessed_cells)} unprocessed cells")
            print(f"Resuming with {len(unprocessed_cells)} unprocessed cells")
            
            # Sort remaining cells by likely business density
            unprocessed_cells = self.sort_grid_cells_by_density(unprocessed_cells)
            
            # Process each remaining cell
            for i, cell in enumerate(unprocessed_cells):
                # Check if we've reached max results
                if max_results and len(self.results) >= max_results:
                    logger.info(f"Reached maximum results ({max_results}), stopping")
                    print(f"Reached maximum results ({max_results}), stopping")
                    break
                
                cell_id = cell["cell_id"]
                
                # Print progress
                print(f"\nProcessing grid cell {i+1}/{len(unprocessed_cells)} ({cell_id})...")
                
                # Process this grid cell
                processed_count = self.process_grid_cell(query, cell)
                
                # Print progress stats
                if processed_count > 0:
                    print(f"Found {processed_count} businesses in grid cell {cell_id}")
                else:
                    print(f"No businesses found in grid cell {cell_id}")
                
                print(f"Total so far: {len(self.results)} businesses ({self.stats['grid_cells_processed']}/{self.stats['grid_cells_total']} cells processed)")
                
                # Update grid visualization
                self.update_grid_visualization()
                
                # Wait between cells to avoid rate limiting
                time.sleep(1)
            
            # Calculate stats
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() / 60  # in minutes
            unique_businesses = len(set((r["name"], r.get("address", "")) for r in self.results))
            
            logger.info(f"✅ GRID SCRAPING COMPLETE")
            logger.info(f"Found {len(self.results)} businesses ({unique_businesses} unique)")
            logger.info(f"Processed {self.stats['grid_cells_processed']} grid cells")
            logger.info(f"Duration: {duration:.2f} minutes")
            
            print("\n===== GRID SCRAPING COMPLETE =====")
            print(f"✅ Found {len(self.results)} businesses ({unique_businesses} unique)")
            print(f"Processed {self.stats['grid_cells_processed']} grid cells")
            print(f"Duration: {duration:.2f} minutes")
            print(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # Final save
            self.save_results()
            
            return self.results
            
        except KeyboardInterrupt:
            logger.warning("⚠️ Grid scraping interrupted by user")
            print("\n⚠️ Grid scraping interrupted by user")
            print("Saving collected data...")
            self.save_results()
            return self.results
            
        except Exception as e:
            logger.error(f"❌ Error during grid scraping: {e}")
            print(f"\n❌ Error during grid scraping: {e}")
            print("Saving any collected data...")
            traceback.print_exc()
            self.save_results()
            return self.results
    
    def close(self):
        """Close the browser and cleanup resources"""
        if hasattr(self, 'driver') and self.driver:
            logger.info("Closing browser...")
            
            try:
                self.driver.quit()
                logger.info("Browser closed successfully")
            except Exception as e:
                logger.error(f"Error closing browser: {e}")
        
        # Save final results
        if hasattr(self, 'results') and self.results:
            try:
                self.save_results()
                logger.info(f"Final save completed with {len(self.results)} businesses")
            except Exception as e:
                logger.error(f"Error during final save: {e}")

# Main function to run the scraper
def run_enhanced_grid_scraper():
    """Run the enhanced grid scraper"""
    print("\n🌍 ENHANCED GOOGLE MAPS GRID SCRAPER 🌍")
    print("=========================================")
    
    # Ask about headless mode
    headless_mode = input("\nRun in headless mode (hidden browser)? (y/n, default: n): ").strip().lower() == 'y'
    
    # Ask about debug mode
    debug_mode = input("Enable debug mode with screenshots? (y/n, default: y): ").strip().lower() != 'n'
    
    # Ask about max tabs
    try:
        max_tabs_input = input("\nHow many tabs to use for email extraction? (1-5, default: 3): ").strip()
        max_tabs = 3  # Default
        if max_tabs_input:
            max_tabs = max(1, min(5, int(max_tabs_input)))
    except ValueError:
        max_tabs = 3
        print(f"Using default value: {max_tabs} tabs")
    
    # Create the scraper instance
    try:
        print("\nInitializing scraper...")
        scraper = EnhancedGoogleMapsGridScraper(headless=headless_mode, max_tabs=max_tabs, debug=debug_mode)
        
        # Ask if resume from previous session
        resume = input("\nResume from previous session? (y/n, default: n): ").strip().lower() == 'y'
        
        if resume:
            # Get session files
            results_file = input("Enter path to results JSON file: ").strip()
            grid_file = input("Enter path to grid JSON file: ").strip()
            
            if not results_file:
                results_file = "google_maps_data.json"
            
            if not grid_file:
                # Try to find the most recent grid file
                grid_files = [f for f in os.listdir("logs") if f.startswith("grid_") and f.endswith(".json")]
                if grid_files:
                    grid_files.sort(reverse=True)  # Most recent first
                    grid_file = os.path.join("logs", grid_files[0])
                    print(f"Using most recent grid file: {grid_file}")
            
            # Load previous session
            if scraper.load_and_resume(results_file, grid_file):
                # Ask for query
                query = input("\nWhat type of business to search for? (e.g., hotels, restaurants): ")
                
                # Ask about maximum results
                try:
                    max_results_input = input("Maximum results to collect (leave empty for unlimited): ").strip()
                    max_results = int(max_results_input) if max_results_input else None
                except ValueError:
                    max_results = None
                    print(f"Using unlimited results")
                
                # Resume scraping
                results = scraper.resume_scraping(query, max_results)
            else:
                print("Could not load previous session, starting new one")
                resume = False
        
        if not resume:
            # Get search parameters
            query = input("\nWhat type of business? (e.g., hotels, restaurants): ")
            location = input("Location (e.g., Prague, New York): ")
            
            # Ask about grid size
            try:
                grid_size = int(input("Grid size in meters (200-500, default: 250): "))
                grid_size = max(200, min(500, grid_size))
            except ValueError:
                grid_size = 250
                print(f"Using default grid size: {grid_size} meters")
            
            # Ask about maximum results
            try:
                max_results_input = input("Maximum results to collect (leave empty for unlimited): ").strip()
                max_results = int(max_results_input) if max_results_input else None
            except ValueError:
                max_results = None
                print(f"Using unlimited results")
            
            # Run the grid scraper
            results = scraper.scrape(query, location, grid_size, max_results)
        
        if results:
            print(f"\n✅ Successfully scraped {len(results)} businesses!")
            print(f"Data saved to:")
            print(f"- 'google_maps_data.csv'")
            print(f"- 'google_maps_data.json'")
            if PANDAS_AVAILABLE:
                print(f"- 'google_maps_data.xlsx'")
            print(f"- 'results/google_maps_data_{scraper.session_id}.*'")
        else:
            print("\n⚠️ No results found")
        
        print("\nDebug logs are saved in the 'logs' folder")
        print("Detailed grid information is in: " + grid_log_filename)
        print("Grid visualization is available at: logs/grid_progress_" + scraper.session_id + ".html")
        print("\nPress Enter to close the browser and exit...")
        input()
        
    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user")
        if 'scraper' in locals():
            scraper.save_results()
            print("Saved current results")
    
    except Exception as e:
        print(f"\n❌ Critical error: {e}")
        print("Check logs for details")
        traceback.print_exc()
    
    finally:
        # Always close the browser
        if 'scraper' in locals():
            scraper.close()
            print("Browser closed")
    
    print("\n👋 Thank you for using Enhanced Google Maps Grid Business Scraper!")

# Run the script if executed directly
if __name__ == "__main__":
    run_enhanced_grid_scraper()